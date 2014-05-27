package riak

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	//"encoding/json"
	. "github.com/mozilla-services/heka/message"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"strings"
	"testing"
	"time"
)

func TestAllSpecs(t *testing.T) {
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(RiakOutputSpec)

	gs.MainGoTest(r, t)
}

func getTestMessageWithFunnyFields() *Message {
	field, _ := NewField(`"foo`, "bar\n", "")
	field1, _ := NewField(`"number`, 64, "")
	field2, _ := NewField("\xa3", "\xa3", "")
	field3, _ := NewField("idField", "1234", "")

	msg := &Message{}
	msg.SetType("TEST")
	t, _ := time.Parse("2006-01-02T15:04:05.000Z", "2013-07-16T15:49:05.070Z")
	msg.SetTimestamp(t.UnixNano())
	msg.SetUuid(uuid.Parse("87cf1ac2-e810-4ddf-a02d-a5ce44d13a85"))
	msg.SetLogger("GoSpec")
	msg.SetSeverity(int32(6))
	msg.SetPayload("Test Payload")
	msg.SetEnvVersion("0.8")
	msg.SetPid(14098)
	msg.SetHostname("hostname")
	msg.AddField(field)
	msg.AddField(field1)
	msg.AddField(field2)
	msg.AddField(field3)

	return msg
}

func RiakOutputSpec(c gs.Context) {
	c.Specify("Should properly encode special characters in json", func() {
		buf := bytes.Buffer{}
		writeStringField(true, &buf, `hello"bar`, "world\nfoo\\")
		c.Expect(buf.String(), gs.Equals, `"hello\u0022bar":"world\u000afoo\u005c"`)
	})

	c.Specify("Should replace invalid utf8 with replacement character", func() {
		buf := bytes.Buffer{}
		writeStringField(true, &buf, "\xa3", "\xa3")
		c.Expect(buf.String(), gs.Equals, "\"\xEF\xBF\xBD\":\"\xEF\xBF\xBD\"")
	})

	c.Specify("Should properly encode message using payload formatter", func() {
		formatter := PayloadFormatter{}
		msg := getTestMessageWithFunnyFields()
		jsonPayload := `{"this": "is", "a": "test"}
		{"of": "the", "payload": "formatter"}`
		msg.SetPayload(jsonPayload)
		b, err := formatter.Format(msg)
		c.Expect(err, gs.IsNil)
		c.Expect(string(b), gs.Equals, jsonPayload)
	})

	c.Specify("Should interpolate fields and message attributes for index and type names", func() {
		interpolatedIndex, err := interpolateFlag(&RiakCoordinates{},
			getTestMessageWithFunnyFields(), "heka-%{Pid}-%{\"foo}-%{2006.01.02}")
		interpolatedType, err := interpolateFlag(&RiakCoordinates{},
			getTestMessageWithFunnyFields(), "%{Type}")
		t := time.Now()

		c.Expect(err, gs.Equals, nil)
		c.Expect(interpolatedIndex, gs.Equals, "heka-14098-bar\n-"+t.Format("2006.01.02"))
		c.Expect(interpolatedType, gs.Equals, "TEST")
	})

	c.Specify("Should interpolate id specified in config", func() {
		var conf RiakOutputConfig
		conf.Id = "%{idField}"
		interpolatedId, err := interpolateFlag(&RiakCoordinates{},
			getTestMessageWithFunnyFields(), conf.Id)
		c.Expect(interpolatedId, gs.Equals, "1234")

		//Test if Id field does not interpolate
		conf.Id = "%{idFail}"
		unInterpolatedId, err := interpolateFlag(&RiakCoordinates{},
			getTestMessageWithFunnyFields(), conf.Id)
		c.Expect(strings.Contains(err.Error(),
			"Could not interpolate field from config: %{idFail}"), gs.Equals, true)
		c.Expect(unInterpolatedId, gs.Equals, "idFail")
	})
}
