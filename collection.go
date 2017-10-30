package kekcollections

import (
	"github.com/MoonBabyLabs/kek"
	"time"
	"github.com/metal3d/go-slugify"
	"github.com/revel/modules/csrf/app"
	"github.com/rs/xid"
	"strings"
	"errors"
	"github.com/MoonBabyLabs/revchain"
	"github.com/MoonBabyLabs/kekstore"
	"log"
	"os"
)

const COLLECTION_PATH = "c/"
const SLUG_PATH = "slugs/"

type Collection struct {
	store kekstore.Storer
	ResourceIds map[string]bool `json:"resource_ids"`
	Slug string `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Id string `json:"id"`
	Name string `json:"name"`
	Description string `json:"description"`
	Revisions revchain.ChainMaker `json:"revisions"`
	Rev string `json:"rev"`
	Docs map[string]kek.Doc `json:"docs"`
	Collections map[string]Collection `json:"collections"`
}

var loadedCol chan *Collection

// Store returns back the collection store location.
func (c Collection) Store() kekstore.Storer {
	return c.store
}

// SetStore sets the location for which to store the collections.
// By default, Collections will use the kekstore.Store which stores content in the ~/.kek directory.
// You probably should avoid setting the store unless you are testing or are positive you want to change the location that you are storing your kek collections.
func (c Collection) SetStore(store kekstore.Storer) Collection {
	c.store = store

	return c
}

func generateSlug(c *Collection) string {
	var slug string

	if c.Slug != "" {
		slug = slugify.Marshal(c.Slug)
	} else if c.Name != "" {
		slug = slugify.Marshal(c.Name)
	} else {
		rand, _ := csrf.RandomString(9)
		slug = rand
	}

	c.Slug = strings.ToLower(slug)
	doesntExist := false
	log.Print(slug)

	for !doesntExist {
		items, _ := c.store.List(SLUG_PATH + c.Slug)

		if len(items) == 0 {
			doesntExist = true
			continue
		}
		rand, _ := csrf.RandomString(3)
		c.Slug = slug + "-" + rand
	}

	return slug
}

// New generates and stores a new Collection item based on the current store location.
// You can optionally specify the initial name, description and resourceIds or just leave zero values.
// It will create the remaining properties of CreatedAt, UpdatedAt, Id, Slug, Revision & Revisions. You shouldn't ever set these values.
func (c Collection) New(name, description string, resourceIds map[string]bool) (Collection, error) {
	if c.store == nil {
		c.store = kekstore.Store{}
	}

	c.Name = name
	c.Description = description
	c.ResourceIds = resourceIds
	c.CreatedAt = time.Now()
	c.UpdatedAt = time.Now()
	c.Id = "cc" + xid.New().String()

	if c.Revisions == nil {
		chain, cErr := revchain.Chain{}.New(c.Id, c)

		if cErr != nil {
			return c, cErr
		}

		c.Rev = chain.GetHashString()
	}

	for id, isIncluded := range c.ResourceIds {
		if !isIncluded {
			delete(c.ResourceIds, id)
		}
	}

	generateSlug(&c)
	c.saveSlug()

	return c, c.store.Save(COLLECTION_PATH + c.Id, c)
}


func (c *Collection) saveSlug() error {
	return c.store.Save(SLUG_PATH + "/" + c.Slug + "/" + c.Id, []byte{})
}

func (c Collection) LoadById(id string, withResources, withRevisions bool) (Collection, error) {
	if c.store == nil {
		c.store = kekstore.Store{}
	}

	err := c.store.Load(COLLECTION_PATH + id, &c)

	if err != nil {
		return c, err
	}

	if withResources {
		c.Docs = make(map[string]kek.Doc)
		c.Collections = make(map[string]Collection)

		for resourceId := range c.ResourceIds {
			typeVal := resourceId[0:2]

			switch typeVal {
			case "cc":
				col := Collection{}
				c.store.Load(COLLECTION_PATH + resourceId, &col)
				c.Collections[resourceId] = col
				break
			case "dd":
				kd := kek.Doc{}
				c.store.Load(kek.DOC_DIR + resourceId, &kd)
				c.Docs[resourceId] = kd
				break
			}
		}
	}

	if withRevisions {
		if c.Revisions == nil {
			revisions, revErr := revchain.Chain{}.SetStore(c.Store()).Load(c.Id)

			if revErr != nil {
				return c, revErr
			}

			c.Revisions = revisions
		} else {
			loadedRev, revErr := c.Revisions.Load(c.Id)

			if revErr != nil {
				return c, revErr
			}

			c.Revisions = loadedRev
		}
	}

	return c, nil
}

func (c Collection) LoadBySlug(slug string, withResources, withKek bool) (Collection, error) {
	if c.store == nil {
		c.store = kekstore.Store{}
	}

	collectionIds, err := c.store.List(SLUG_PATH + slug)

	if err != nil {
		return c, err
	}

	earliestWhen := time.Now()
	earliestColId := ""

	for id := range collectionIds {
		xResourceId := id[2:]
		la, err := xid.FromString(xResourceId)

		if err != nil {
			return c, err
		}

		if la.Time().Unix() < earliestWhen.Unix() {
			earliestColId = id
		}
	}

	return c.LoadById(earliestColId, withResources, withKek)
}

func (c Collection) Delete(delRev bool) (error) {
	if c.store == nil {
		c.store = kekstore.Store{}
	}


	delDone := make(chan error, 3)
	col, _ := c.LoadById(c.Id, false, false)

	go func() {
		delDone <- c.store.Delete(COLLECTION_PATH + col.Id)
	}()

	if delRev {
		go func() {
			if c.Revisions == nil {
				c.Revisions = revchain.Chain{}.SetStore(c.Store())
			}
			delDone <- c.Revisions.Delete(col.Id)
		}()
	} else {
		delDone <- nil
	}

	go func() {
		delDone <- os.RemoveAll(SLUG_PATH + col.Slug)
	}()

	for i := 0; i < 3; i++ {
		select {
		case delErr := <-delDone:
			if delErr != nil {
				return delErr
			}
		}
	}

	return nil
}

func (c Collection) All(withDocs bool, withKek bool) (map[string]Collection, error) {
	if c.store == nil {
		c.store = kekstore.Store{}
	}

	cols := make(map[string]Collection)
	contents, loadErr := c.store.List(COLLECTION_PATH)

	if loadErr != nil {
		return cols, loadErr
	}

	for item := range contents {
		colIdChars := item[0:2]
		if colIdChars == "cc" && !strings.Contains(item, ".kek") {
			cCol, colLoadEr := c.LoadById(item, withDocs, withKek)

			if colLoadEr != nil {
				return cols, colLoadEr
			}

			cols[item] = cCol
		}
	}

	return cols, nil
}

func (c Collection) loadCol(col *Collection) {
	if c.store == nil {
		c.store = kekstore.Store{}
	}

	c.store.Load(c.Id, &col)
	loadedCol <- col
}

func (c Collection) Save() error {
	if c.store == nil {
		c.store = kekstore.Store{}
	}

	if c.Revisions == nil {
		revLoad, revLoadErr := revchain.Chain{}.Load(c.Id)
		if revLoadErr != nil {
			return revLoadErr
		}

		addedBlock, addRevErr := revLoad.AddBlock(c.Id, c)

		if addRevErr != nil {
			return addRevErr
		}

		c.Rev = addedBlock.GetHashString()
	} else{
		revLoad, revLoadErr := c.Revisions.Load(c.Id)
		if revLoadErr != nil {
			return revLoadErr
		}

		addedBlock, addRevErr := revLoad.AddBlock(c.Id, c)

		if addRevErr != nil {
			return addRevErr
		}

		c.Rev = addedBlock.GetHashString()
	}

	c.UpdatedAt = time.Now()

	return c.store.Save(COLLECTION_PATH + c.Id, c)
}

func (c *Collection) AddResource(resourceId string) error {
	if len(c.ResourceIds) == 0 {
		c.ResourceIds = map[string]bool{}
	}

	c.ResourceIds[resourceId] = true

	return c.Save()
}

func (c *Collection) DeleteResource(resourceId string) error {
	if len(c.ResourceIds) < 1 || !c.ResourceIds[resourceId] {
		return errors.New("Cannot remove kekresource: " + resourceId + " because an association doesn't currently exist")
	}

	delete(c.ResourceIds, resourceId)

	return c.Save()
}