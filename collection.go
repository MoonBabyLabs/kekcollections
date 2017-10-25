package kekcollections

import (
	"github.com/MoonBabyLabs/kek"
	"time"
	"github.com/MoonBabyLabs/revchain"
	"github.com/metal3d/go-slugify"
	"github.com/revel/modules/csrf/app"
	"encoding/json"
	"github.com/rs/xid"
	"strings"
	"errors"
	"strconv"
)

const COLLECTION_PATH = "c/"
const SLUG_PATH = "slugs/"

type Collection struct {
	store kek.Storer
	ResourceIds map[string]bool `json:"resource_ids"`
	Slug string `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Id string `json:"id"`
	KekId string `json:"kekid"`
	Name string `json:"name"`
	Description string `json:"description"`
	Revisions revchain.Chain `json:"revisions"`
	Rev string `json:"rev"`
	Docs map[string]kek.Doc `json:"docs"`
	Collections map[string]Collection `json:"collections"`
}

var loadedCol chan *Collection
var loadedChain chan bool

func (c Collection) Store() kek.Storer {
	return c.store
}

func (c Collection) SetStore(store kek.Storer) Collection {
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
	baseSlug := c.Slug

	for doesntExist {
		_, empty := c.store.List(COLLECTION_PATH + c.Slug)
		if empty != nil {
			doesntExist = true
			continue
		}
		rand, _ := csrf.RandomString(3)
		c.Slug = baseSlug + "-" + rand
	}

	return slug
}

func (c Collection) New() error {
	blockString := make(chan string)
	c.CreatedAt = time.Now()
	c.UpdatedAt = time.Now()
	c.Id = "cc" + xid.New().String()

	for id, isIncluded := range c.ResourceIds {
		if !isIncluded {
			delete(c.ResourceIds, id)
		}
	}

	c.createChain(blockString)
	c.Rev = <-blockString
	generateSlug(&c)
	c.saveSlug()

	return c.store.Save(COLLECTION_PATH + c.Id, c)
}

func (c Collection) createChain(blockString chan string) error {
	revSaved := make(chan bool)
	ks, ksErr := kek.Kekspace{}.Load()

	if ksErr != nil {
		return ksErr
	}

	chain := revchain.Chain{}
	docIdBytes, _ := json.Marshal(c.ResourceIds)
	blck := revchain.Block{}.New(ks, docIdBytes, "", 0)
	chain = chain.New(blck)
	go c.saveRev(chain, revSaved)
	<-revSaved
	blockString <- blck.HashString()

	return nil
}

func (c Collection) saveRev(chain revchain.Chain, revChan chan bool) {
	if c.store == nil {
		c.store = kek.Store{}
	}

	c.store.Save(COLLECTION_PATH + c.Id + ".kek", chain)
	revChan <- true
}

func (c Collection) loadChain(chain *revchain.Chain) {
	if c.store == nil {
		c.store = kek.Store{}
	}

	c.store.Load(COLLECTION_PATH + c.Id + ".kek", &chain)
	loadedChain <- true
}

func (c *Collection) saveSlug() error {
	if c.store == nil {
		c.store = kek.Store{}
	}

	return c.store.Save(SLUG_PATH + "/" + c.Slug + "/" + c.Id, []byte{})
}

func (c Collection) LoadById(id string, withResources, withRevisions bool) (Collection, error) {
	if c.store == nil {
		c.store = kek.Store{}
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
		revisions := revchain.Chain{}
		c.store.Load(COLLECTION_PATH + id + ".kek", &revisions)
	}

	return c, nil
}

func (c Collection) LoadBySlug(slug string, withResources, withKek bool) (Collection, error) {
	if c.store == nil {
		c.store = kek.Store{}
	}

	collectionIds, err := c.store.List(SLUG_PATH + slug)

	if err != nil {
		return c, err
	}

	earliestWhen := time.Now()
	earliestColId := ""

	for id := range collectionIds {
		la, err := xid.FromString(id)

		if err != nil {
			continue
		}

		if la.Time().Unix() < earliestWhen.Unix() {
			earliestColId = id
		}
	}

	return c.LoadById(earliestColId, withResources, withKek)
}

func (c *Collection) Delete(delRev bool) (error) {
	if c.store == nil {
		c.store = kek.Store{}
	}

	delDone := make(chan error, 3)
	col, _ := c.LoadById(c.Id, false, false)

	go func() {
		delDone <- c.store.Delete(COLLECTION_PATH + col.Id)
	}()

	if delRev {
		go func() {
			delDone <- c.store.Delete(COLLECTION_PATH + col.Id + ".kek")
		}()
	} else {
		delDone <- nil
	}

	go func() {
		delDone <- c.store.Delete(COLLECTION_PATH + col.Slug + "/" + col.Id)
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

func (c *Collection) Replace() error {
	revUpdate := make(chan string)
	go c.AddRevision(revUpdate)
	c.UpdatedAt = time.Now()
	c.Slug = generateSlug(c)
	c.Rev =  <-revUpdate

	return c.Save()
}

func (c Collection) All(withDocs bool, withKek bool) (map[string]Collection, error) {
	if c.store == nil {
		c.store = kek.Store{}
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
		c.store = kek.Store{}
	}

	c.store.Load(c.Id, &col)
	loadedCol <- col
}

func (c *Collection) Merge(oldColChan chan Collection) {
	if c.store == nil {
		c.store = kek.Store{}
	}

	oldCol := Collection{}
	c.store.Load(COLLECTION_PATH + c.Id, &oldCol)

	if c.Name != "" && c.Name != oldCol.Name {
		oldCol.Name = c.Name
	}
	
	if c.Slug != "" && c.Slug != oldCol.Slug {
		oldCol.Slug = c.Slug
	} else if c.Slug == "" && oldCol.Slug == "" {
		oldCol.Slug = generateSlug(c)
	}

	if oldCol.ResourceIds == nil {
		oldCol.ResourceIds = make(map[string]bool)
	}

	for docKey, include := range c.ResourceIds {
		if include {
			oldCol.ResourceIds[docKey] = true
		} else {
			delete(oldCol.ResourceIds, docKey)
		}
	}

	if c.Description != "" {
		oldCol.Description = c.Description
	}

	oldColChan <- oldCol
}

func (c *Collection) Patch() error {
	if c.store == nil {
		c.store = kek.Store{}
	}

	oldColChan := make( chan Collection)
	c.Merge(oldColChan)

	return c.Save()
}

func (c *Collection) AddRevision(revDone chan string) (string, error) {
	if c.store == nil {
		c.store = kek.Store{}
	}

	chain := revchain.Chain{}
	ks := kek.Kekspace{}
	ksErr := c.store.Load(kek.KEK_SPACE_CONFIG, &ks)
	
	if ksErr != nil {
		return "", ksErr
	}
	
	err := c.store.Load(COLLECTION_PATH + c.Id + ".kek", &chain)

	if err != nil {
		return "", err
	}
	
	lastBlock := chain.GetLast()
	block := revchain.Block{}.New(ks, c, lastBlock.HashString(), lastBlock.Index + 1)
	chain.AddBlock(block)
	saveRev := make(chan bool)
	c.saveRev(chain, saveRev)
	revDone <- block.HashString()

	return block.HashString(), nil
}

func (c Collection) Save() error {
	if c.store == nil {
		c.store = kek.Store{}
	}

	c.UpdatedAt = time.Now()
	revDone := make(chan string)
	latestRev, err := c.AddRevision(revDone)

	if err != nil {
		return err
	}

	c.Rev = latestRev

	return c.store.Save(COLLECTION_PATH + c.Id, c)
}

func (c *Collection) AddDoc(kd kek.Doc) error {
	if c.store == nil {
		c.store = kek.Store{}
	}

	if len(c.ResourceIds) == 0 {
		c.ResourceIds = map[string]bool{}
	}

	c.ResourceIds[kd.Id] = true

	return c.Save()
}


func (c *Collection) DeleteDoc(kd kek.Doc) error {
	if c.store == nil {
		c.store = kek.Store{}
	}

	if len(c.ResourceIds) < 1 || !c.ResourceIds[kd.Id] {
		return errors.New("Cannot remove kekresource: " + kd.Id + " because an association doesn't currently exist")
	}

	return c.Save()
}