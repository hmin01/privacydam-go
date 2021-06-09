package core

import (
	"encoding/json"

	// Model
	"privacydam-go/v1/core/model"
	// DB
	"privacydam-go/v1/core/db"
)

func EmptyEvaluation() model.Evaluation {
	return model.Evaluation{}
}

func EmptyApi() model.Api {
	return model.Api{}
}

func EmptySource() model.Source {
	return model.Source{}
}

func TransformToApi(rawSource interface{}) *model.Api {
	return rawSource.(*model.Api)
}

func TransformToDidOptions(rawOptions string) (map[string]model.AnoParamOption, error) {
	// Set default de-identification options
	var didOptions map[string]model.AnoParamOption
	// Transform to structure
	if err := json.Unmarshal([]byte(rawOptions), &didOptions); err != nil {
		return didOptions, err
	} else {
		return didOptions, nil
	}
}

func GetInternalDatabase() (model.ConnInfo, error) {
	return db.GetDatabase("internal", nil)
}

func GetExternalDatabase(key interface{}) (model.ConnInfo, error) {
	return db.GetDatabase("external", key)
}
