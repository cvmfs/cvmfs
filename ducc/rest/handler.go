package rest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/cvmfs/ducc/db"
	"github.com/google/uuid"
)

const uuidRegex string = "[0-9(a-f|A-F)]{8}-[0-9(a-f|A-F)]{4}-4[0-9(a-f|A-F)]{3}-[89ab][0-9(a-f|A-F)]{3}-[0-9(a-f|A-F)]{12}"

type ctxKey struct{}

// getField returns the field at the given index from the URL path
func getField(r *http.Request, index int) string {
	fields := r.Context().Value(ctxKey{}).([]string)
	return fields[index]
}

// ServeHTTP implements the http.Handler interface for PatternHandler
func (this *PatternHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var allow []string
	for _, route := range this.routes {
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
	NewRoute("GET", "/wishes", getAllWishesHandler),
	NewRoute("POST", "/wishes", notImplementedHandler),
	NewRoute("POST", "/wishes/("+uuidRegex+")/sync", notImplementedHandler),
	NewRoute("GET", "/wishes/("+uuidRegex+")", getWishHandler),
	NewRoute("DELETE", "/wishes/("+uuidRegex+")", notImplementedHandler),
	NewRoute("POST", "/wishes/("+uuidRegex+")/sync", notImplementedHandler),
	NewRoute("GET", "/wishes/("+uuidRegex+")/images", notImplementedHandler),
	NewRoute("GET", "/wishes/("+uuidRegex+")/jobs", notImplementedHandler),

	// Images
	NewRoute("GET", "/images", getAllImagesHandler),
	NewRoute("GET", "/images/("+uuidRegex+")", notImplementedHandler),
	NewRoute("POST", "/images/("+uuidRegex+")/delete", notImplementedHandler),
	NewRoute("POST", "/images/("+uuidRegex+")/sync", notImplementedHandler),
	NewRoute("GET", "/images/("+uuidRegex+")/jobs", notImplementedHandler),

	// Jobs
	NewRoute("GET", "/jobs", notImplementedHandler),
	NewRoute("GET", "/jobs/("+uuidRegex+")", notImplementedHandler),
	NewRoute("POST", "/jobs/("+uuidRegex+")/cancel", notImplementedHandler),

	// Recipes
	NewRoute("POST", "/recipes/(.+)", applyRecipeHandler),

	// Webhooks
	NewRoute("POST", "/webhooks/harbor", notImplementedHandler),

	// HTML
	NewRoute("GET", "/html/jobs/("+uuidRegex+")", getTaskHtmlHandler),
	NewRoute("GET", "/html/images", getImagesHtmlHandler),

	// Other general actions
	// - Clean up orphaned images
	// - Clean up orphaned layers

}}

func RunRestApi() {
	http.HandleFunc("/", handler.ServeHTTP)
	http.ListenAndServe(":8080", nil)
}

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

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Successfully applied recipe. Imported %d wish(es), deleted %d wish(es)", len(newWishes), len(deletedWishes))))
}
