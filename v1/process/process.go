package process

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/labstack/echo"

	// AWS
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-xray-sdk-go/xray"

	// Model
	"privacydam-go/v1/core/model"
	// Util
	"privacydam-go/v1/process/util/auth"
	"privacydam-go/v1/process/util/db"
)

// func ProcessTestInEcho(ctx echo.Context) error {
// 	// Set context
// 	cCtx := ctx.Request().Context()

// 	// Get API information
// 	api, err := GetApiInformation(cCtx, "a_marketing_01")
// 	if err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	}

// 	// Verify API expires
// 	if err := VerifyExpires(cCtx, api.ExpDate, api.Status); err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	}

// 	// Get query parameters and verify parameters
// 	// params := make([]interface{}, 0)
// 	params := []interface{}{"10"}

// 	didOptions, err := GetDeIdentificationOptions(cCtx, api.Uuid)
// 	if err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	}

// 	_, err = db.Ex_exportData(cCtx, ctx.Response(), api, params, didOptions)
// 	if err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	} else {
// 		return nil
// 	}
// }

func TestConnection(ctx context.Context, driverName string, dsn string) error {
	return db.Ex_testConnection(ctx, driverName, dsn)
}

/*
 * Extract API alias from request path and verify alias format (on echo framework)
 * <IN> ctx (echo.Context): context
 * <IN> key (string): key to found api alias from request path
 * <OUT> (string): extracted value (= API alias)
 * <OUT> (error): error object (contain nil)
 */
func ExtractApiAliasOnEcho(ctx echo.Context, key string) (string, error) {
	// Extract
	value := ctx.Param(key)
	// Verify API alias format
	if value != "" {
		// Verify
		err := VerifyApiAliasFormat(value)
		return value, err
	} else {
		return value, errors.New("Not found request path parameter")
	}
}

/*
 * Extract API alias from request path and verify alias format (on AWS lambda)
 * <IN> ctx (context.Context): context
 * <IN> key (string): key to found api alias from request path
 * <OUT> (string): extracted value (= API alias)
 * <OUT> (error): error object (contain nil)
 */
func ExtractApiAliasOnLambda(ctx context.Context, request events.APIGatewayProxyRequest, key string) (string, error) {
	// Extract
	if value, ok := request.PathParameters[key]; ok {
		// Verify
		err := VerifyApiAliasFormat(value)
		return value, err
	} else {
		return value, errors.New("Not found request path parameter")
	}
}

/*
 * Verify API alias format
 * <IN> alias (string): alias value to verify
 * <OUT> (error): error object (contain nil)
 */
func VerifyApiAliasFormat(alias string) error {
	// Verify
	match, err := regexp.MatchString("^a_+", alias)
	if err != nil {
		return err
	} else if !match {
		return errors.New("Invalid API alias format")
	} else {
		return nil
	}
}

/*
 * Get API information
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> param (string): condition to find API (api_alias)
 * <OUT> (model.Api): api information format
 * <OUT> (error): error object (contain nil)
 */
func GetApiInformation(ctx context.Context, tracking bool, param string) (model.Api, error) {
	var subCtx context.Context = ctx
	var subSegment *xray.Segment

	// [For debug] set subsegment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Find API information")
		defer subSegment.Close(nil)
	}

	// Find API using param
	return db.In_findApi(subCtx, param)
}

/*
 * Verify API expires
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> date (string): api expiration date (mysql datatime format)
 * <IN> status (string): api activation status ('active' or 'disabled')
 * <OUT> (error): error object (contain nil)
 */
func VerifyExpires(ctx context.Context, tracking bool, date string, status string) error {
	// [For debug] set subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Verify API parameters")
		defer subSegment.Close(nil)
	}

	// Verify API expires
	expDate, err := time.Parse("2006-01-02 15:04:05", date)
	if expDate.Before(time.Now()) {
		err = errors.New("This API has expired")
	} else if status == "disabled" {
		err = errors.New("This API is not avaliable")
	}
	return err
}

/*
 * Verify API parameters (on echo framework)
 * <IN> ctx (echo.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> keys ([]string): a list of parameter key
 * <OUT> ([]interface{}): a list of parameter value extracted from the request
 * <OUT> (error): error object (contain nil)
 */
func VerifyParametersOnEcho(ctx echo.Context, tracking bool, keys []string) ([]interface{}, error) {
	// [For debug] set subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx.Request().Context(), "Verify API parameters")
		defer subSegment.Close(nil)
	}

	// Get parameters from request body
	params := make([]interface{}, 0)
	for _, key := range keys {
		value := ctx.QueryParam(key)
		if value == "" {
			return params, errors.New("Invalid parameters")
		} else {
			params = append(params, value)
		}
	}
	// Verify parameters
	err := verifyParameters(params, keys)
	return params, err
}

/*
 * Verify API parameters (on AWS lambda)
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> req (events.APIGatewayProxyRequest): request object (for AWS APIGateway proxy, lambda)
 * <IN> keys ([]string): a list of parameter key
 * <OUT> ([]interface{}): a list of parameter value extracted from the request
 * <OUT> (error): error object (contain nil)
 */
func VerifyParametersOnLambda(ctx context.Context, tracking bool, req events.APIGatewayProxyRequest, keys []string) ([]interface{}, error) {
	// [For debug] set subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Verify API parameters")
		defer subSegment.Close(nil)
	}

	// Get parameters from request body
	params := make([]interface{}, 0)
	for _, key := range keys {
		if value, ok := req.QueryStringParameters[key]; ok {
			params = append(params, value)
		} else {
			return params, errors.New("Invalid parameters")
		}
	}
	// Verify parameters
	err := verifyParameters(params, keys)
	return params, err
}

func verifyParameters(standard []interface{}, target []string) error {
	if len(standard) != len(target) {
		return errors.New("Invalid paramters")
	} else {
		return nil
	}
}

/*
 * Get a de-identification options
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> id (string): API id by generated database
 * <OUT> (map[string]model.AnoParamOption): de-identification options
 * <OUT> (error): error object (contain nil)
 */
func GetDeIdentificationOptions(ctx context.Context, tracking bool, id string) (map[string]model.AnoParamOption, error) {
	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	// [For debug] set subsegment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Verify API parameters")
		defer subSegment.Close(nil)
	}

	// Set default de-identification options
	var didOptions map[string]model.AnoParamOption

	// Get de-identification options
	rawOptions, err := db.In_getDeIdentificationOptions(subCtx, id)
	if err != nil {
		return didOptions, err
	}
	// Transform to structure
	if rawOptions != "" {
		err = json.Unmarshal([]byte(rawOptions), &didOptions)
		return didOptions, err
	} else {
		return nil, err
	}
}

/*
 * Authenticate access on server (on echo framework)
 * <IN> ctx (echo.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> server (string): OPA server host (contain protocal, host, port)
 * <OUT> (error): authentication result (nil is a successful authentication)
 */
func AuthenticateAccessOnEcho(ctx echo.Context, tracking bool, server string) error {
	var subCtx context.Context = ctx.Request().Context()
	var subSegment *xray.Segment
	if tracking {
		// [For debug] set subsegment
		subCtx, subSegment = xray.BeginSubsegment(ctx.Request().Context(), "Authentication access")
		defer subSegment.Close(nil)
	}

	// Extract access token
	token, err := auth.ExtractAccessTokenInEcho(ctx)
	if err != nil {
		return err
	}
	// Authenticate access token (using another OPA)
	return auth.AuthenticateAccess(subCtx, tracking, server, token)
}

/*
 * Export data (process for export API)
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> res (http.ResponseWriter): responseWriter object
 * <IN> apiName (string): api name
 * <IN> sourceId (string): api source id by generated database
 * <IN> querySyntax (string) syntax to query
 * <IN> params ([]interface{}): parameters to query
 * <IN> didOptions (map[string]model.AnoParamOption): de-identification options
 * <OUT> (model.Evaluation): k-anonymity evaluation result
 * <OUT> (error): error object (contain nil)
 */
func ExportData(ctx context.Context, tracking bool, res http.ResponseWriter, apiName string, sourceId string, querySyntax string, params []interface{}, didOptions map[string]model.AnoParamOption) (model.Evaluation, error) {
	// Check api name
	name := apiName
	if apiName == "" {
		name = "undefined_apiName"
	}
	// Processing
	return db.Ex_exportData(ctx, tracking, res, name, sourceId, querySyntax, params, didOptions)
}

/*
 * Change data (process for control API)
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> sourceId (string): api source id by generated database
 * <IN> querySyntax (string): syntax to query
 * <IN> params ([]interface{}): parameters to query
 * <IN> isTest (bool): test or not
 * <OUT> (int64): affected row count by query
 * <OUT> (error): error object (contain nil)
 */
func ChangeData(ctx context.Context, tracking bool, sourceId string, querySyntax string, params []interface{}, isTest bool) (int64, error) {
	return db.Ex_changeData(ctx, tracking, sourceId, querySyntax, params, isTest)
}
