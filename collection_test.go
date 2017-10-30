package kekcollections

import (
	"testing"
	"github.com/rs/xid"
	"strings"
	"encoding/json"
	"time"
	"github.com/MoonBabyLabs/kek"
	"github.com/MoonBabyLabs/revchain"
)

type StoreMock struct {

}

type ChainMock struct {

}

func (c ChainMock) New(id string, resourceIdBytes interface{}) (revchain.ChainMaker, error) {
	return c, nil
}

func (c ChainMock) GetHashString() string {
	return "a"
}

func (c ChainMock) AddBlock(id string, resourceIdBytes interface{}) (revchain.ChainMaker, error) {
	return c, nil
}

func (c ChainMock) Load(path string) (revchain.ChainMaker, error) {
	return c, nil
}

func (C ChainMock) Delete(id string) error {
	return nil
}

func (s StoreMock) Load(location string, unmarshallStruct interface{}) error {
	sample := make(map[string]interface{})
	sample["title"] = "a"
	sample["description"] = "b"
	sample["resource_ids"] = map[string]bool{"dda32":true}
	sample["created_at"] = time.Now()
	sample["updated_at"] = time.Now()
	sample["docs"] = map[string]kek.Doc{}
	sample["collections"] = map[string]Collection{}
	sampleData, _ := json.Marshal(sample)
	json.Unmarshal(sampleData, &unmarshallStruct)

	return nil
}

func (s StoreMock) Save(locale string, item interface{}) error {
	return nil
}

func (s StoreMock) Delete(locale string) error {
	return nil
}

func (s StoreMock) List(locale string) (map[string]bool, error){
	return map[string]bool{"ddb7p8vjk1dh027e6olh9g" : true}, nil
}

func TestCollectionNew(t *testing.T) {
	resourceIds := map[string]bool{
		"afasees" : true,
		"basdfe" : true,
		"cceasefe" : false,
		"ddeasse" : true,
		"cceasasdfefe" : true,
	}
	newCol, err := Collection{}.SetStore(StoreMock{}).New("my sample name", "a good description", resourceIds); if err != nil {
		t.Log(err)
		t.Fail()
	}

	xidWithoutCC := newCol.Id[2:len(newCol.Id)]

	id, xEerr := xid.FromString(xidWithoutCC); if xEerr != nil {
		t.Log(xEerr)
		t.Fail()
	}

	if id.Time().IsZero() {
		t.Log("time cannot be zero")
		t.Fail()
	}

	if !newCol.ResourceIds["afasees"] || !newCol.ResourceIds["basdfe"] || !newCol.ResourceIds["ddeasse"] || !newCol.ResourceIds["cceasasdfefe"] {
		t.Log("missing resourceId items")
		t.Fail()
	}

	if newCol.ResourceIds["cceasefe"] {
		t.Log("Resource should not be present in col{}.ResourceId because it was passed with a false boolean")
		t.Fail()
	}

	if strings.Contains(newCol.Slug, " ") {
		t.Log("slug should not contain a space")
		t.Fail()
	}
}

func TestCollection_Delete(t *testing.T) {
	col := Collection{}.SetStore(StoreMock{})
	err := col.Delete(true)

	if err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestCollection_AddRemoveResource(t *testing.T) {
	col := Collection{}.SetStore(StoreMock{})
	col.Revisions = ChainMock{}
	col.AddResource("blah33a")

	if !col.ResourceIds["blah33a"] {
		t.Log("Resource not added")
		t.Fail()
	}

	col.DeleteResource("blah33a")

	if col.ResourceIds["blah33a"] {
		t.Log("Resource is present when should've been deleted")
		t.Fail()
	}
}

func TestCollection_LoadBySlug(t *testing.T) {
	saveCol := Collection{
		Revisions: ChainMock{},
	}.SetStore(StoreMock{})

	col, loadErr := saveCol.LoadBySlug("ddb7p8vjk1dh027e6olh9g", true, true)

	if loadErr != nil {
		t.Log(loadErr)
		t.Log(col)
		t.Fail()
	}

	if col.CreatedAt.IsZero() || col.UpdatedAt.IsZero() {
		t.Log("updatedAt and createdAt date should not be empty on load")
		t.Fail()
	}

	col, loadErr = saveCol.LoadBySlug("ddb7p8vjk1dh027e6olh9g", false, true)

	if loadErr != nil {
		t.Log(loadErr)
		t.Log(col)
		t.Fail()
	}

	if col.CreatedAt.IsZero() || col.UpdatedAt.IsZero() {
		t.Log("updatedAt and createdAt date should not be empty on load")
		t.Fail()
	}
}

func TestCollection_Patch(t *testing.T) {

}