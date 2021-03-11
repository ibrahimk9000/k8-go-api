package controllers

import (
	"errors"
	"testing"
)

const (
	contentManagementFlagJSON = `{
		"PdfContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "Javascript": 1,
		  "Acroform": 1,
		  "ActionsAll": 1
		},
		"ExcelContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "DynamicDataExchange": 1,
		  "Macros": 1,
		  "ReviewComments": 1
		},
		"PowerPointContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "Macros": 1,
		  "ReviewComments": 1
		},
		"WordContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "DynamicDataExchange": 1,
		  "Macros": 1,
		  "ReviewComments": 1
		}
	}`

	mcontentManagementFlagJSON = `{
		"PdfContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "Javascript": 1,
		  "Acroform": 1,
		  "ActionsAl": 1     
		},
		"ExcelContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "DynamicDataExchange": 1,
		  "Macros": 1,
		  "ReviewComments": 1
		},
		"PowerPointContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "Macros": 1,
		  "ReviewComments": A
		},
		"WordContentManagement": {
		  "Metadata": 1,
		  "InternalHyperlinks": 1,
		  "ExternalHyperlinks": 1,
		  "EmbeddedFiles": 1,
		  "EmbeddedImages": 1,
		  "DynamicDataExchange": 1,
		  "Macros": 1,
		  "ReviewComments": 1
		}
	}`
)

const (
	SampleDataPath = "../sampledata/"
	PdfFileName    = "file.pdf"
	ZipFileName    = "file.zip"
)

//ignore enpoint tests for the moment until we restructure them to itegration tests

func TestParsecontentManagementFlagJSON(t *testing.T) {

	njson := []byte(contentManagementFlagJSON)
	ejson := []byte{}
	mjson := []byte(mcontentManagementFlagJSON)

	notnil := func(i error) bool {
		if i != nil {
			return true
		}
		return false
	}

	var tests = []struct {
		c  []byte
		er bool
	}{
		{njson, notnil(nil)},
		{ejson, notnil(errors.New(""))},
		{mjson, notnil(errors.New(""))},
	}

	for _, test := range tests {
		if _, err := parseContentManagementFlagJSON(test.c); notnil(err) != test.er {
			t.Errorf("Test Failed: Error {%s} ", err)

		}
	}
}
