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
)

const COLLECTION_PATH = "c/"

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

	c.Slug = slug

	return slug
}


func (c Collection) New() (Collection, error) {
	c.CreatedAt = time.Now()
	c.UpdatedAt = time.Now()
	ks := kek.Kekspace{}
	kek.Load(kek.KEK_SPACE_CONFIG, &ks)
	chain := revchain.Chain{}

	for id, isIncluded := range c.ResourceIds {
		if !isIncluded {
			delete(c.ResourceIds, id)
		}
	}

	docIdBytes, _ := json.Marshal(c.ResourceIds)

	blck := revchain.Block{}.New(ks, docIdBytes, "", 0)
	chain = chain.New(blck)
	c.Id = "cc" + xid.New().String()
	c.Rev = blck.HashString()
	generateSlug(&c)
	kek.Save(COLLECTION_PATH + c.Id, c)
	kek.Save(COLLECTION_PATH + "/" + c.Slug + "/" + c.Id, []byte{})
	kek.Save(COLLECTION_PATH + c.Id + ".rev", chain)

	return c, nil
}

func (c Collection) LoadById(id string, withDocs, withRevisions bool) (Collection, error) {
	kek.Load(COLLECTION_PATH + id, &c)

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
		kek.Load(COLLECTION_PATH + id + ".rev", &revisions)
	}

	return c, nil
}

func (c Collection) LoadBySlug(slug string, collectionItem int, withDocs, withRevisions bool) (Collection, error) {
	collectionIds, err := kek.List(COLLECTION_PATH + slug, -1)
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
		kek.Load(COLLECTION_PATH + c.Id + ".rev", &revisions)
	}

	return c, nil
}

func (c *Collection) Delete(delRev bool) (error) {
	kek.Delete(COLLECTION_PATH + c.Id)

	if delRev {
		kek.Delete(COLLECTION_PATH + c.Id + ".rev")
	}

	return kek.Delete(COLLECTION_PATH + "/" + c.Slug + "/" + c.Id)
}

func (c *Collection) Replace() (*Collection, error) {
	oldCol := Collection{}
	kek.Load(c.Id, &oldCol)
	oldCol.Delete(false)
	rev, revErr := c.AddRevision()
	c.UpdatedAt = time.Now()
	c.Slug = generateSlug(c)
	c.Rev = rev

	if revErr != nil {
		return c, revErr
	}

	return c, kek.Save(COLLECTION_PATH + c.Id, c)
}

func (c *Collection) Patch(newCol Collection) (*Collection, error) {
	oldCol := Collection{}
	kek.Load(COLLECTION_PATH + c.Id, &oldCol)
	oldCol.UpdatedAt = time.Now()

	if c.Name != "" && c.Name != oldCol.Name {
		oldCol.Name = c.Name
	}

	if c.Slug != "" && c.Slug != oldCol.Slug {
		oldCol.Slug = c.Slug
	} else if c.Slug == "" && oldCol.Slug == "" {
		oldCol.Slug = generateSlug(c)
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


	rev, revErr :=  oldCol.AddRevision()

	if revErr != nil {
		return c, revErr
	}

	c.Rev = rev


	kek.Save(COLLECTION_PATH + oldCol.Id, oldCol)

	return c, nil
}

func (c *Collection) AddRevision() (string, error) {
	chain := revchain.Chain{}
	ks := kek.Kekspace{}
	_, ksErr := kek.Load(kek.KEK_SPACE_CONFIG, &ks)

	if ksErr != nil {
		return "", ksErr
	}
	_, err := kek.Load(COLLECTION_PATH + c.Id + ".rev", &chain)

	if err != nil {
		return "", err
	}

	lastBlock := chain.GetLast()
	block := revchain.Block{}.New(ks, c, lastBlock.HashString(), lastBlock.Index + 1)
	chain.AddBlock(block)
	kek.Save(COLLECTION_PATH + c.Id + ".rev", chain)

	return block.HashString(), nil
}

func (c Collection) Save() error {
	return kek.Save(COLLECTION_PATH + c.Id, c)
}

func (c Collection) AddDoc(kd kek.KekDoc) (Collection, error) {
	c.UpdatedAt = time.Now()
	c.ResourceIds[kd.Id] = true
	chain := revchain.Chain{}
	block := revchain.Block{}
	ks := kek.Kekspace{}
	kek.Load(COLLECTION_PATH + c.Id + ".rev", &chain)
	kek.Load(kek.KEK_SPACE_CONFIG, &ks)
	lastBlock := chain.GetLast()
	docBytes, _ := json.Marshal(c.ResourceIds)
	newBlock := block.New(ks, docBytes, lastBlock.HashString(), lastBlock.Index)
	chain.AddBlock(newBlock)
	c.Rev = chain.GetLast().HashString()
	kek.Save(COLLECTION_PATH + c.Id, c)
	kek.Save(COLLECTION_PATH + c.Id + ".rev", chain)

	return c, nil
}

func (c Collection) DeleteDoc(kd kek.KekDoc) (Collection, error) {
	c.UpdatedAt = time.Now()
	chain := revchain.Chain{}
	block := revchain.Block{}
	ks := kek.Kekspace{}
	kek.Load(COLLECTION_PATH + c.Id + ".rev", &chain)
	kek.Load(kek.KEK_SPACE_CONFIG, &ks)
	lastBlock := chain.GetLast()
	newBlock := block.New(ks, []byte(kd.Id), lastBlock.HashString(), lastBlock.Index)
	chain.AddBlock(newBlock)
	delete(c.ResourceIds, kd.Id)
	kek.Save(COLLECTION_PATH + c.Id + ".rev", chain)
	kek.Save(COLLECTION_PATH + c.Id, c)

	return c, nil
}