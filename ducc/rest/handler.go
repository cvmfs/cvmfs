package rest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/errorcodes"
	"github.com/google/uuid"
)

const uuidRegex string = "[0-9(a-f|A-F)]{8}-[0-9(a-f|A-F)]{4}-4[0-9(a-f|A-F)]{3}-[89ab][0-9(a-f|A-F)]{3}-[0-9(a-f|A-F)]{12}"
const optionalTrailingSlashesRegex string = "/*"

type ctxKey struct{}

// getField returns the field at the given index from the URL path
func getField(r *http.Request, index int) string {
	fields := r.Context().Value(ctxKey{}).([]string)
	return fields[index]
}

// ServeHTTP implements the http.Handler interface for PatternHandler
func (ph PatternHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var allow []string
	for _, route := range ph.routes {
		matches := route.pattern.FindStringSubmatch(r.URL.Path)
		if len(matches) > 0 {
			if r.Method != route.method {
				allow = append(allow, route.method)
				continue
			}
			ctx := context.WithValue(r.Context(), ctxKey{}, matches[1:])
			route.handler(w, r.WithContext(ctx))
			return
		}
	}
	if len(allow) > 0 {
		w.Header().Set("Allow", strings.Join(allow, ", "))
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.NotFound(w, r)
}

func StartRestServer(done chan<- any, bindPort int, bindInterface string) *http.Server {
	srv := &http.Server{Addr: fmt.Sprintf("%s:%d", bindInterface, bindPort), Handler: handler}

	go func() {
		defer close(done)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("REST server error: %s", err)
			os.Exit(errorcodes.RESTServerError)
		}
	}()

	return srv
}

func RunRestApi(ctx context.Context, bindPort int, bindInterface string) {
	http.HandleFunc("/", handler.ServeHTTP)
	http.ListenAndServe(fmt.Sprintf("%s:%d", bindInterface, bindPort), nil)
}

type patternRoute struct {
	method  string
	pattern *regexp.Regexp
	handler http.HandlerFunc
}

func NewRoute(method, pattern string, handler http.HandlerFunc) patternRoute {
	// Precompile patterns for better performance
	return patternRoute{method, regexp.MustCompile("^" + pattern + "$"), handler}
}

type PatternHandler struct {
	routes []patternRoute
}

// By defining the routes in a variable, we can easily test them
var handler = PatternHandler{[]patternRoute{
	// Wishes
	NewRoute("GET", "/api/v1/wishes/*", getAllWishesHandler),
	NewRoute("POST", "/api/v1/wishes/("+uuidRegex+")/sync", notImplementedHandler),
	NewRoute("GET", "/api/v1/wishes/("+uuidRegex+")", getWishHandler),
	NewRoute("POST", "/api/v1/wishes/("+uuidRegex+")/sync", notImplementedHandler),
	NewRoute("GET", "/api/v1/("+uuidRegex+")/images", notImplementedHandler),
	NewRoute("GET", "/api/v1/("+uuidRegex+")/jobs", notImplementedHandler),

	// Images
	NewRoute("GET", "/api/v1/images/*", getAllImagesHandler),
	NewRoute("GET", "/api/v1/images/("+uuidRegex+")", notImplementedHandler),
	NewRoute("POST", "/api/v1/images/("+uuidRegex+")/delete", notImplementedHandler),
	NewRoute("POST", "/api/v1/images/("+uuidRegex+")/sync", notImplementedHandler),
	NewRoute("GET", "/api/v1/images/("+uuidRegex+")/jobs", notImplementedHandler),

	// Jobs
	NewRoute("GET", "/api/v1/tasks/*", notImplementedHandler),
	NewRoute("GET", "/api/v1/tasks/("+uuidRegex+")", notImplementedHandler),
	NewRoute("POST", "/api/v1/tasks/("+uuidRegex+")/cancel", notImplementedHandler),

	// Recipes
	NewRoute("POST", "/api/v1/recipes/(.+)", applyRecipeHandler),

	// Webhooks
	NewRoute("POST", "/api/v1/webhooks/harbor", notImplementedHandler),

	// HTML
	NewRoute("GET", "/", frontPageHtmlHandler),
	NewRoute("GET", "/tasks/("+uuidRegex+")", getTaskHtmlHandler),
	NewRoute("GET", "/operations/*", operationsHtmlHandler),
	NewRoute("GET", "/images/*", getImagesHtmlHandler),

	// Other general actions
	// - Clean up orphaned images
	// - Clean up orphaned layers

}}

func notImplementedHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("Not implemented"))
}

// getWishHandler is the endpoint for GET `/wishes/<wish-id>`
func getWishHandler(w http.ResponseWriter, r *http.Request) {
	uuid, err := uuid.Parse(getField(r, 0))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	wish, err := db.GetWishByID(nil, uuid)
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Convert from internal type to API type
	images, err := db.GetImagesByWishID(nil, wish.ID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	restWish := CreateApiWishFromDBWish(wish, images)
	restWishJSON, err := json.Marshal(restWish)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(restWishJSON)
}

// getAllWishesHandler is the endpoint for GET `/wishes`
func getAllWishesHandler(w http.ResponseWriter, r *http.Request) {
	wishes, err := db.GetAllWishes(nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	wishIDs := make([]db.WishID, len(wishes))
	for i, wish := range wishes {
		wishIDs[i] = wish.ID
	}
	wishImages, err := db.GetImagesByWishIDs(nil, wishIDs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Convert from internal type to API type
	restWishes := make([]Wish, len(wishes))
	for i, wish := range wishes {
		restWish := CreateApiWishFromDBWish(wish, wishImages[i])
		restWishes[i] = restWish
	}
	wishesJson, err := json.Marshal(restWishes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(wishesJson)
}

// getAllImagesHandler is the endpoint for GET `/images`
func getAllImagesHandler(w http.ResponseWriter, r *http.Request) {
	images, err := db.GetAllImages(nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	imageIDs := make([]db.ImageID, len(images))
	for i, image := range images {
		imageIDs[i] = image.ID
	}
	linkedWishes, err := db.GetWishesByImageIDs(nil, imageIDs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// Convert from internal type to API type
	restImages := make([]Image, len(images))
	for i, image := range images {
		restImages[i] = CreateApiImageFromDBImage(image, linkedWishes[i])
	}
	imagesJson, err := json.Marshal(restImages)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(imagesJson)
}

func applyRecipeHandler(w http.ResponseWriter, r *http.Request) {
	source := getField(r, 0)

	// TODO: Validate source string

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse the recipe
	recipe, err := db.ParseYamlRecipeV1(body, source)
	if err != nil {
		fmt.Printf("Invalid recipe: %s\n", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Invalid recipe: %s", err.Error())))
		return
	}
	// Import the recipe
	newWishes, deletedWishes, err := db.ImportRecipeV1(recipe)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// TODO: Schedule update of the new wishes
	/*for _, wish := range newWishes {
		// TODO: daemon.TriggerCheck(wish.ID, db.OP_TYPE_EXPAND_WILDCARDS, db.TRIGGER_TYPE_MANUAL, fmt.Sprintf("Recipe %s applied", source))
	}*/

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Successfully applied recipe. Imported %d wish(es), deleted %d wish(es)", len(newWishes), len(deletedWishes))))
}
