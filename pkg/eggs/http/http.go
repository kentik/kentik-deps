package http

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/kentik/eggs/pkg/logger"
)

// Helpers is a collection of http handler helpers. It is
// intended to be embedded into the object with your routing and handlers.
type Helpers interface {
	WithErrJSONBodyVarInt(string, BodyIntHandlerFunc) http.HandlerFunc

	WithErrAndJSON(ErrHandlerFunc) http.HandlerFunc
	WithBody(BodyHandlerFunc) ErrHandlerFunc
	WithVarInt(string, BodyIntHandlerFunc) BodyHandlerFunc

	WithLogging(http.Handler) http.Handler

	HandleErrAndJSON(http.ResponseWriter, interface{}, error)
	SendInternalServerError(http.ResponseWriter, string)
	SendBadRequest(http.ResponseWriter, string)
	SendNotFound(http.ResponseWriter, string)
	SendJSONResponse(http.ResponseWriter, interface{})
}

// NewHelpers constructs a Helpers.
func NewHelpers(log logger.ContextL, me markErrors, f varsFunc) Helpers {
	return &helpers{
		MarkErrors: me,
		ContextL:   log,
		varsFunc:   f,
	}
}

type helpers struct {
	MarkErrors markErrors
	logger.ContextL
	varsFunc varsFunc
}

type markErrors func(int64) // e.g. chf/metrics/MitigateSerivceMetric.Errors
// NoopMarkErrors is a noop func for marking errors. Can pass to NewHelpers.
func NoopMarkErrors(_ int64) {}

type varsFunc func(*http.Request) map[string]string

var GorillaVarsFunc = mux.Vars

func (h *helpers) WithErrJSONBodyVarInt(varName string, handler BodyIntHandlerFunc) http.HandlerFunc {
	return h.WithErrAndJSON(h.WithBody(h.WithVarInt(varName, handler)))
}

// ErrHandlerFunc is like http.HandlerFunc but with a return value and error.
type ErrHandlerFunc func(http.ResponseWriter, *http.Request) (interface{}, error)

// BodyHandlerFunc is like ErrHandlerFunc but with the request body as well.
type BodyHandlerFunc func(http.ResponseWriter, *http.Request, []byte) (interface{}, error)

func (h *helpers) WithBody(handler BodyHandlerFunc) ErrHandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("While reading request body: %+v", err)
		}

		return handler(w, r, bodyBytes)
	}
}

// BodyIntHandlerFunc is like ErrHandlerFunc, plus the request body,
// plus a gorilla/mux var converted to an int.
type BodyIntHandlerFunc func(http.ResponseWriter, *http.Request, []byte, int) (interface{}, error)

func (h *helpers) WithVarInt(varName string, handler BodyIntHandlerFunc) BodyHandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, body []byte) (interface{}, error) {
		vars := h.varsFunc(r)
		i, err := strconv.Atoi(vars[varName])
		if err != nil {
			return nil, &BadRequestError{M: fmt.Sprintf("Couldn't parse %s='%s' (%+v)", varName, vars[varName], err)}
		}

		return handler(w, r, body, i)
	}
}

func (h *helpers) WithErrAndJSON(handler ErrHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		obj, err := handler(w, r)
		h.HandleErrAndJSON(w, obj, err)
	}
}

func (h *helpers) HandleErrAndJSON(w http.ResponseWriter, obj interface{}, err error) {
	if err != nil {
		switch e := err.(type) {
		case *BadRequestError:
			h.SendBadRequest(w, e.Error())
		case *NotFoundError:
			h.SendNotFound(w, e.Error())
		default:
			h.SendInternalServerError(w, "500 Error: "+err.Error())
		}
		return
	}

	h.SendJSONResponse(w, obj)
}

func (h *helpers) WithLogging(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Infof("Handling request: %s %s (RemoteAddr=%s)", r.Method, r.URL, r.RemoteAddr)
		handler.ServeHTTP(w, r)
		h.Debugf("Handled request: %s %s (RemoteAddr=%s)", r.Method, r.URL, r.RemoteAddr)
	})
}

// BadRequestError causes a handler wrapped in WithErrAndJSON to return a 400.
type BadRequestError struct{ M string }

func (bre *BadRequestError) Error() string { return "400 Bad Request: " + bre.M }

// NotFoundError causes a handler wrapped in WithErrAndJSON to return a 404.
type NotFoundError struct{ M string }

func (nfe *NotFoundError) Error() string { return "404 Not Found: " + nfe.M }

func (h *helpers) SendInternalServerError(w http.ResponseWriter, msg string) {
	h.MarkErrors(1)
	h.Errorf("%s", msg)
	http.Error(w, msg, http.StatusInternalServerError)
}

func (h *helpers) SendBadRequest(w http.ResponseWriter, msg string) {
	h.MarkErrors(1)
	h.Warnf("%s", msg)
	http.Error(w, msg, http.StatusBadRequest)
}

func (h *helpers) SendNotFound(w http.ResponseWriter, msg string) {
	h.MarkErrors(1)
	h.Warnf("%s", msg)
	http.Error(w, msg, http.StatusNotFound)
}

func (h *helpers) SendJSONResponse(w http.ResponseWriter, response interface{}) {
	bs, err := json.Marshal(response)
	if err != nil {
		h.SendInternalServerError(w, fmt.Sprintf("500 Marshalling: %v", err))
		return
	}

	if _, err := w.Write(bs); err != nil {
		h.MarkErrors(1)
		h.Errorf("Writing in SendJSONResponse: %v", err)
	}
}

// OKResponse is a generic JSON response.
type OKResponse struct {
	Result  string `json:"result"`
	Message string `json:"message,omitempty"`
}

// AllOK is a JSON response for when everything is fine.
var AllOK = OKResponse{Result: "OK", Message: ""}
