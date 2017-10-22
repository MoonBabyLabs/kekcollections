package kekcollections

import (
	"github.com/MoonBabyLabs/kek"
	"time"
	"github.com/MoonBabyLabs/revchain"
	"github.com/metal3d/go-slugify"
	"github.com/revel/modules/csrf/app"
	"encoding/json"
	"log"
	"github.com/rs/xid"
	"strings"
	"errors"
)

const COLLECTION_PATH = "c/"
const SLUG_PATH = "slugs/"

type Collection struct {
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
	Docs map[string]kek.KekDoc `docs:"docs"`
}

var loadedCol chan *Collection
var loadedChain chan bool

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

	return slug
}


func (c Collection) New() (Collection, error) {
	blockString := make(chan string)
	c.CreatedAt = time.Now()
	c.UpdatedAt = time.Now()
	c.Id = "cc" + xid.New().String()

	for id, isIncluded := range c.ResourceIds {
		if !isIncluded {
			delete(c.ResourceIds, id)
		}
	}

	go c.createChain(blockString)
	c.Rev = <-blockString
	generateSlug(&c)
	slugSaved := make(chan bool)
	go c.saveSlug(slugSaved)
	cChan := make(chan bool)
	go c.saveCol(cChan)
	<- cChan
	<- slugSaved

	return c, nil
}

func (c Collection) createChain(blockString chan string) {
	revSaved := make(chan bool)
	ks := kek.Kekspace{}
	chain := revchain.Chain{}
	kek.Load(kek.KEK_SPACE_CONFIG, &ks)
	docIdBytes, _ := json.Marshal(c.ResourceIds)
	blck := revchain.Block{}.New(ks, docIdBytes, "", 0)
	chain = chain.New(blck)
	go c.saveRev(chain, revSaved)
	<-revSaved
	blockString <- blck.HashString()
}


func (c Collection) saveRev(chain revchain.Chain, revChan chan bool) {
	
	kek.Save(COLLECTION_PATH + c.Id + ".kek", chain)
	
	revChan <- true
	
}

func (c Collection) saveCol(colChan chan bool) {
	kek.Save(COLLECTION_PATH + c.Id, c)
	colChan <- true
}

func (c Collection) loadKekspace(ks chan kek.Kekspace) kek.Kekspace {
	kekSp := kek.Kekspace{}
	sp, _ := kek.Load(kek.KEK_SPACE_CONFIG, kekSp)
	ks <- sp.(kek.Kekspace)

	return sp.(kek.Kekspace)
}

func (c Collection) loadChain(chain *revchain.Chain) {
	kek.Load(COLLECTION_PATH + c.Id + ".kek", &chain)
	loadedChain <- true
}

func (c *Collection) saveSlug(slugSaved chan bool) {
	kek.Save(SLUG_PATH + "/" + c.Slug + "/" + c.Id, []byte{})
	slugSaved <- true
}

func (c Collection) LoadById(id string, withDocs, withRevisions bool) (Collection, error) {
	_, err := kek.Load(COLLECTION_PATH + id, &c)

	if err != nil {
		return c, err
	}

	if withDocs {
		c.Docs = make(map[string]kek.KekDoc)

		for docId := range c.ResourceIds {
			kd := kek.KekDoc{}
			kek.Load(kek.DOC_DIR + docId, &kd)
			c.Docs[id] = kd
		}
	}

	if withRevisions {
		revisions := revchain.Chain{}
		kek.Load(COLLECTION_PATH + id + ".kek", &revisions)
	}

	return c, nil
}

func (c Collection) LoadBySlug(slug string, collectionItem int, withDocs, withRevisions bool) (Collection, error) {
	collectionIds, err := kek.List(SLUG_PATH + slug, -1)
	count := 0

	if err != nil {
		return c, err
	}

	for id := range collectionIds {
		if count == collectionItem {
			kek.Load(COLLECTION_PATH + id, &c)
			break
		}
	}

	if withDocs {
		c.Docs = make(map[string]kek.KekDoc)

		for id := range c.ResourceIds {
			kd := kek.KekDoc{}
			_, docLoadErr := kek.Load(kek.DOC_DIR + id, &kd)

			if docLoadErr == nil {
				c.Docs[id] = kd
			}
		}
	}

	if withRevisions {
		revisions := revchain.Chain{}
		kek.Load(COLLECTION_PATH + c.Id + ".kek", &revisions)
	}

	return c, nil
}

func (c *Collection) Delete(delRev bool) (error) {
	delDone := make(chan error, 3)
	col, _ := c.LoadById(c.Id, false, false)

	go func() {
		delDone <- kek.Delete(COLLECTION_PATH + col.Id)
	}()

	if delRev {
		go func() {
			delDone <- kek.Delete(COLLECTION_PATH + col.Id + ".kek")
		}()
	} else {
		delDone <- nil
	}

	go func() {
		delDone <- kek.Delete(COLLECTION_PATH + col.Slug + "/" + col.Id)
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

func (c *Collection) Replace() (*Collection, error) {
	revUpdate := make(chan string)
	saveCol := make(chan bool)
	go c.AddRevision(revUpdate)
	c.UpdatedAt = time.Now()
	c.Slug = generateSlug(c)
	c.Rev =  <-revUpdate
	go c.saveCol(saveCol)
	<-saveCol

	return c, nil
}

func (c Collection) All(withDocs bool, withKek bool) (map[string]Collection, error) {
	cols := make(map[string]Collection)

	contents, loadErr := kek.List(COLLECTION_PATH, -1)

	if loadErr != nil {
		return cols, loadErr
	}

	for item, _ := range contents {
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
	kek.Load(c.Id, &col)
	loadedCol <- col
}

func (c *Collection) Merge(oldColChan chan Collection) {
	oldCol := Collection{}
	kek.Load(COLLECTION_PATH + c.Id, &oldCol)
	log.Print(c)
	log.Print(oldCol)
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
			log.Print(docKey)
			delete(oldCol.ResourceIds, docKey)
		}
	}

	if c.Description != "" {
		oldCol.Description = c.Description
	}

	oldColChan <- oldCol
}

func (c *Collection) Patch() (*Collection, error) {
	oldColChan := make( chan Collection)
	go c.Merge(oldColChan)
	oldCol := <-oldColChan
	oldCol.UpdatedAt = time.Now()
	revString := make(chan string)
	go oldCol.AddRevision(revString)
	oldCol.Rev = <- revString
	colSaved := make(chan bool)
	go oldCol.saveCol(colSaved)
	<-colSaved

	return c, nil
}

func (c *Collection) AddRevision(revDone chan string) (string, error) {
	chain := revchain.Chain{}
	ks := kek.Kekspace{}
	_, ksErr := kek.Load(kek.KEK_SPACE_CONFIG, &ks)
	
	if ksErr != nil {
		return "", ksErr
	}
	
	_, err := kek.Load(COLLECTION_PATH + c.Id + ".kek", &chain)

	if err != nil {
		return "", err
	}
	
	lastBlock := chain.GetLast()
	block := revchain.Block{}.New(ks, c, lastBlock.HashString(), lastBlock.Index + 1)
	chain.AddBlock(block)
	saveRev := make(chan bool)
	go c.saveRev(chain, saveRev)
	<-saveRev
	revDone <- block.HashString()

	return block.HashString(), nil
}

func (c Collection) Save() error {
	return kek.Save(COLLECTION_PATH + c.Id, c)
}

func (c *Collection) AddDoc(kd kek.KekDoc) (*Collection, error) {
	c.UpdatedAt = time.Now()

	if len(c.ResourceIds) == 0 {
		c.ResourceIds = map[string]bool{}
	}

	c.ResourceIds[kd.Id] = true
	chain := revchain.Chain{}
	block := revchain.Block{}
	ks, _ := kek.Kekspace{}.Load()
	kek.Load(COLLECTION_PATH + c.Id + ".kek", &chain)
	lastBlock := chain.GetLast()
	docBytes, _ := json.Marshal(c.ResourceIds)
	newBlock := block.New(ks, docBytes, lastBlock.HashString(), lastBlock.Index)
	chain.AddBlock(newBlock)
	c.Rev = newBlock.HashString()
	kek.Save(COLLECTION_PATH + c.Id, c)
	kek.Save(COLLECTION_PATH + c.Id + ".kek", chain)

	return c, nil
}


func (c *Collection) DeleteDoc(kd kek.KekDoc) (*Collection, error) {

	if len(c.ResourceIds) < 1 || !c.ResourceIds[kd.Id] {
		return c, errors.New("Cannot remove kekdoc: " + kd.Id + " because it already isn't an association.")
	}

	c.UpdatedAt = time.Now()
	chain := revchain.Chain{}
	block := revchain.Block{}
	ks, _ := kek.Kekspace{}.Load()
	kek.Load(COLLECTION_PATH + c.Id + ".kek", &chain)
	lastBlock := chain.GetLast()
	docBytes, _ := json.Marshal(c.ResourceIds)
	newBlock := block.New(ks, docBytes, lastBlock.HashString(), lastBlock.Index)
	chain.AddBlock(newBlock)
	delete(c.ResourceIds, kd.Id)
	c.Rev = newBlock.HashString()
	kek.Save(COLLECTION_PATH + c.Id + ".kek", chain)
	kek.Save(COLLECTION_PATH + c.Id, c)

	return c, nil
}