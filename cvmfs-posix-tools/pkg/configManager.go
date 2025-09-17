package pkg

import (
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	pathlib "github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
	yaml "gopkg.in/yaml.v2"
)

type ConfUserStruct struct {
	AllowUpload      bool `yaml:"allow-upload"`
	AllowPurge       bool `yaml:"allow-purge"`
	AllowDelete      bool `yaml:"allow-delete"`
	AllowAclFlag     bool `yaml:"allow-acl"`
	User             int  `yaml:"user"` //This is the uid
	Group            int  `yaml:"group"`
	FileMode         int  `yaml:"file-mode"`
	DirectoryMode    int  `yaml:"directory-mode"`
	CheckPermissions bool `yaml:"check-permissions"`
}

type RepoFields struct {
	Proxy                      string `yaml:"proxy"`
	S3AccessKey                string `yaml:"s3_access_key"`
	S3SecretKey                string `yaml:"s3_secret_key"`
	S3Prefix                   string
	S3Bucket                   string
	S3Endpoint                 string
	DotScheme                  bool   `yaml:"dot-scheme"`
	ContentAddressable         bool   `yaml:"content-addressable"`
	ExternalUrlPrefix          string `yaml:"external-url-prefix"`
	InternalUrlPrefix          string `yaml:"internal-url-prefix"`
	UrlPrefix                  string
	AlternateS3AccessKey       string `yaml:"s3_mutable_bucket_access_key"`
	AlternateS3SecretKey       string `yaml:"s3_mutable_bucket_access_secret"`
	AlternateS3Prefix          string
	AlternateS3Bucket          string
	AlternateS3Endpoint        string
	AlternateExternalUrlPrefix string `yaml:"mutable-external-url-prefix"`
	AlternateInternalUrlPrefix string `yaml:"mutable-internal-url-prefix"`
	AlternateUrlPrefix         string
	AlternateBucketPathStrings []string `yaml:"s3_mutable_paths"`
	AlternateBucketPaths       []*pathlib.Path
	Groups                     map[string]map[string]ConfUserStruct `yaml:"groups"`
	GroupConfig                map[string]ConfUserStruct            // We don't really need this , but it makes the code look better and it's not that much duplication
	CurrentGroupConfig         ConfUserStruct
	NoHeadObjectCheck          bool `yaml:"no-head-object-check"`
}

type ConfStruct struct {
	Repos      map[string]RepoFields `yaml:"repos"`
	Repo       RepoFields
	PathPrefix string
}

type RepoSliceStruct struct {
	Groups yaml.MapSlice `yaml:"groups"`
}

type ConfGroupsSliceStruct struct {
	Repos map[string]RepoSliceStruct `yaml:"repos"`
}

func GetPathExecuteBits(stat fs.FileInfo) (user, group, others bool) {
	mode := stat.Mode()
	user = mode&0100 != 0
	group = mode&0010 != 0
	others = mode&0001 != 0
	return user, group, others
}

// Get the owner, group, mode of the passed in path info
func GetPathPerms(stat fs.FileInfo) (owner int, group int, mode int, err error) {
	mode = int(stat.Mode())
	if sysStat, ok := stat.Sys().(*syscall.Stat_t); ok {
		owner := int(sysStat.Uid)
		group := int(sysStat.Gid)
		return owner, group, mode, nil
	} else {
		err := fmt.Errorf("could not cast to syscall.Stat_t")
		log.Error().Err(err).Msg("check stat.Sys()")
		return 0, 0, 0, err
	}
}

// Get the owner, group, and modes for the passed in configuration's path
func PermsForGroup(cfg ConfStruct) (owner int, group int, filemode int, dirmode int) {
	path := cfg.PathPrefix
	groupconfig, ok := cfg.Repo.GroupConfig[path]
	log.Debug().Str("path", path).Bool("ok", ok).Msg("permsForGroup has entry for path?")
	for !ok && path != "." {
		path = pathlib.NewPath(path).Parent().Clean().String()
		groupconfig, ok = cfg.Repo.GroupConfig[path]
		log.Debug().Str("path", path).Bool("ok", ok).Msg("permsForGroup has entry for path?")
	}
	if !ok {
		log.Info().Msg("No matching group configuration")
	}
	if path == "." {
		path = DefaultGroupPath
		groupconfig, ok = cfg.Repo.GroupConfig[path]
		if !ok {
			GiveNoPermDefault(cfg, path)
		}
	}

	owner = groupconfig.User
	group = groupconfig.Group
	filemode = groupconfig.FileMode
	dirmode = groupconfig.DirectoryMode

	log.Debug().Str("path", path).Int("owner", owner).Int("group", group).Int("filemode", filemode).Int("dirmode", dirmode).Msg("permsForGroup found")
	return
}

// Get the expected owner, group, mode based on the environment's setup
func GetPermsForUpload(cfg ConfStruct, srcStat fs.FileInfo, file bool, acls ACLFlag) (owner int, group int, mode int, err error) {
	switch acls {
	case ACLNone:
		owner, group, filemode, dirmode := PermsForGroup(cfg)
		if file {
			return owner, group, filemode, nil
		} else {
			return owner, group, dirmode, nil
		}
	case ACLPreserveAll:
		if !cfg.Repo.CurrentGroupConfig.AllowAclFlag {
			err := fmt.Errorf("acl flag not allowed for path")
			log.Error().Err(err).Str("Path", cfg.PathPrefix).Msg("Cannot use acl flag in path")
			return 0, 0, 0, err
		}
		owner, group, mode, err := GetPathPerms(srcStat)
		if err != nil {
			return 0, 0, 0, err
		}
		return owner, group, mode, nil
	case ACLPreserveExec:
		owner, group, filemode, dirmode := PermsForGroup(cfg)
		if file {
			exeUser, exeGroup, exeOthers := GetPathExecuteBits(srcStat)
			if exeUser {
				filemode |= 0100
			}
			if exeGroup {
				filemode |= 0010
			}
			if exeOthers {
				filemode |= 0001
			}
			return owner, group, filemode, nil
		} else {
			return owner, group, dirmode, nil
		}
	case ACLPreserveMode:
		if !cfg.Repo.CurrentGroupConfig.AllowAclFlag {
			err := fmt.Errorf("acl flag not allowed for path")
			log.Error().Err(err).Str("Path", cfg.PathPrefix).Msg("Cannot use acl flag in path")
			return 0, 0, 0, err
		}
		owner, group, _, _ := PermsForGroup(cfg)
		_, _, mode, err := GetPathPerms(srcStat)
		if err != nil {
			return 0, 0, 0, err
		}
		return owner, group, mode, nil
	case ACLPreserveOwner:
		if !cfg.Repo.CurrentGroupConfig.AllowAclFlag {
			err := fmt.Errorf("acl flag not allowed for path")
			log.Error().Err(err).Str("Path", cfg.PathPrefix).Msg("Cannot use acl flag in path")
			return 0, 0, 0, err
		}
		_, _, filemode, dirmode := PermsForGroup(cfg)
		owner, group, _, err := GetPathPerms(srcStat)
		if err != nil {
			return 0, 0, 0, err
		}
		if file {
			return owner, group, filemode, nil
		} else {
			return owner, group, dirmode, nil
		}
	default:
		return 0, 0, 0, fmt.Errorf("Unknown ACL flag %v", acls)
	}
}

// Get the uidString, uid, a map of the groupIds to name and list of those names from the environment
func GetUserGroupInfo() (string, int, map[int]bool, map[string]bool, error) {
	currentUser, err := user.Current()
	if err != nil {
		log.Error().Err(err).Msg("Received error while getting the current user")
		return "", 0, nil, nil, err
	}
	groupIds, err := currentUser.GroupIds()
	if err != nil {
		log.Error().Err(err).Msg("Received error while getting users groupids")
		return "", 0, nil, nil, err
	}
	groupNames := make(map[string]bool)
	groupIdMap := make(map[int]bool)
	for _, groupId := range groupIds {
		group, err := user.LookupGroupId(groupId)
		if err != nil {
			log.Error().Err(err).Msg("Received error while looking up groupid")
			return "", 0, nil, nil, err
		}
		groupNames[group.Name] = true
		groupIdInt, err := strconv.Atoi(groupId)
		if err != nil {
			log.Error().Err(err).Str("GroupId", groupId).Msg("Could not convert groupId to str")
			return "", 0, nil, nil, err
		}
		groupIdMap[groupIdInt] = true
	}

	uidInt, err := strconv.Atoi(currentUser.Uid)
	if err != nil {
		log.Error().Err(err).Str("Uid", currentUser.Uid).Msg("Could not convert uid to str")
		return "", 0, nil, nil, err
	}
	uidString := currentUser.Uid
	return uidString, uidInt, groupIdMap, groupNames, nil
}

func parseS3BucketInfo(prefix string) (string, string, string, string, error) {
	cvmfsUrl, err := url.Parse(prefix)
	if err != nil {
		log.Error().Err(err).Str("Url", prefix).Msg("Failed to parse URL")
		return "", "", "", "", err
	}
	endpoint := cvmfsUrl.Host
	bucketPrefixPath := cvmfsUrl.Path
	if bucketPrefixPath[0:1] == FileDelimeter {
		bucketPrefixPath = bucketPrefixPath[1:]
	}
	bucketPrefixPathList := strings.Split(bucketPrefixPath, FileDelimeter)
	bucket := bucketPrefixPathList[0]
	s3Prefix := strings.Join(bucketPrefixPathList[1:], FileDelimeter) + FileDelimeter
	return prefix, endpoint, bucket, s3Prefix, nil
}

// Add the S3 url parsed out info to the passed configuration
func addS3Info(cvmfsConfInfo *ConfStruct) error {
	var err error
	cvmfsConfInfo.Repo.UrlPrefix, cvmfsConfInfo.Repo.S3Endpoint, cvmfsConfInfo.Repo.S3Bucket, cvmfsConfInfo.Repo.S3Prefix, err = parseS3BucketInfo(cvmfsConfInfo.Repo.ExternalUrlPrefix)
	if cvmfsConfInfo.Repo.ContentAddressable {
		cvmfsConfInfo.Repo.UrlPrefix, cvmfsConfInfo.Repo.S3Endpoint, cvmfsConfInfo.Repo.S3Bucket, cvmfsConfInfo.Repo.S3Prefix, err = parseS3BucketInfo(cvmfsConfInfo.Repo.InternalUrlPrefix)
	}
	if err != nil {
		return err
	}
	if cvmfsConfInfo.Repo.AlternateExternalUrlPrefix != "" && !cvmfsConfInfo.Repo.ContentAddressable {
		cvmfsConfInfo.Repo.AlternateUrlPrefix, cvmfsConfInfo.Repo.AlternateS3Endpoint, cvmfsConfInfo.Repo.AlternateS3Bucket, cvmfsConfInfo.Repo.AlternateS3Prefix, err = parseS3BucketInfo(cvmfsConfInfo.Repo.AlternateExternalUrlPrefix)
	}
	if cvmfsConfInfo.Repo.AlternateInternalUrlPrefix != "" && cvmfsConfInfo.Repo.ContentAddressable {
		cvmfsConfInfo.Repo.AlternateUrlPrefix, cvmfsConfInfo.Repo.AlternateS3Endpoint, cvmfsConfInfo.Repo.AlternateS3Bucket, cvmfsConfInfo.Repo.AlternateS3Prefix, err = parseS3BucketInfo(cvmfsConfInfo.Repo.AlternateInternalUrlPrefix)
	}
	alternateBucketPaths := []*pathlib.Path{}
	for _, pathString := range cvmfsConfInfo.Repo.AlternateBucketPathStrings {
		alternateBucketPaths = append(alternateBucketPaths, pathlib.NewPath(pathString))
	}
	cvmfsConfInfo.Repo.AlternateBucketPaths = alternateBucketPaths
	return nil
}

// Unmarshal the byte slice of configuration info and determine the configuration info from repo and groups
func buildCvmfsConfInfo(cvmfsConf []byte, repoName string, groupNames map[string]bool) (ConfStruct, error) {
	var cvmfsConfInfo ConfStruct

	// Unmarshall config file
	if err := yaml.Unmarshal(cvmfsConf, &cvmfsConfInfo); err != nil {
		log.Error().Err(err).Msg("Error unmarshaling config file")
		return cvmfsConfInfo, err
	}

	// Set convenience Repo field
	var contains bool
	if cvmfsConfInfo.Repo, contains = cvmfsConfInfo.Repos[repoName]; !contains {
		err := fmt.Errorf("dest path is not a cvmfs repo")
		log.Error().Err(err).Msg("Received error while determining repo")
		return cvmfsConfInfo, err
	}

	// Add s3 bucket info (parsed from external url)
	if err := addS3Info(&cvmfsConfInfo); err != nil {
		return cvmfsConfInfo, err
	}

	// Unmarshall same file into groupsSlice. This allows for priority traversal
	var cvmfsConfGroupsSliceInfo ConfGroupsSliceStruct
	if err := yaml.Unmarshal(cvmfsConf, &cvmfsConfGroupsSliceInfo); err != nil {
		log.Error().Err(err).Msg("Error unmarshaling file for groups slice")
		return cvmfsConfInfo, err
	}

	// Based on priority, match group and set that as GroupConfig (a convenience field)

	cvmfsConfInfo.Repo.GroupConfig = make(map[string]ConfUserStruct)

	log.Debug().Msg("Extracting group path configuration:")
	found := false
	for _, item := range cvmfsConfGroupsSliceInfo.Repos[repoName].Groups {
		if _, contains := groupNames[item.Key.(string)]; contains {
			for key, val := range cvmfsConfInfo.Repo.Groups[item.Key.(string)] {
				log.Debug().Str("group", item.Key.(string)).Str("path", key).Msgf("Adding config  %+v", val)
				if _, exists := cvmfsConfInfo.Repo.GroupConfig[key]; exists {
					log.Warn().Msgf("Duplicate path config for %v (group %v)", key, item.Key.(string))
				} else {
					cvmfsConfInfo.Repo.GroupConfig[key] = val
					found = true
				}
			}
		}
	}
	if found {
		return cvmfsConfInfo, nil
	}

	cvmfsConfInfo.Repo.GroupConfig = cvmfsConfInfo.Repo.Groups[DefaultGroup]
	return cvmfsConfInfo, nil
}

func GetConfigFileForRepo(repoName string, override bool, overrideFilename string) *pathlib.Path {
	if override {
		if overrideFilename == "" {
			return pathlib.NewPath(CVMFSConfigFileOverride)
		} else {
			return pathlib.NewPath(overrideFilename)
		}
	}
	return pathlib.NewPath(ConfigFilePrefix).Join(repoName).Join(CVMFSConfigFileSuffix)
}

// Get the cvmfs configuration info as well as the uid string, uid int, and a map of groupIds to names
func GetCvmfsConfigurationInfo(repoName string, cvmfsConfFile *pathlib.Path) (ConfStruct, string, int, map[int]bool, error) {
	log.Info().Str("Path", cvmfsConfFile.String()).Msg("Reading config from path")

	cvmfsConf, err := cvmfsConfFile.ReadFile()
	if err != nil {
		if os.IsPermission(err) {
			uid := os.Getuid()
			user, _ := user.LookupId(strconv.Itoa(uid))
			hostname, _ := os.Hostname()
			log.Error().Err(err).Str("file", cvmfsConfFile.String()).Int("User ID", uid).Str("User", user.Username).Str("Hostname", hostname).Msg("Permission Error")

			permPath := cvmfsConfFile
			for permPath.String() != "/" {
				exists, _, err := PathExists(permPath)
				if err != nil {
					log.Error().Err(err).Str("Path", permPath.String()).Msg("File Error")
					// return ConfStruct{}, "", 0, nil, err
				} else if !exists {
					err = fmt.Errorf("file does not exist")
					log.Error().Err(err).Str("Path", permPath.String()).Msg("File Does Not Exist")
					// return ConfStruct{}, "", 0, nil, err
				}
				permPath = permPath.Parent()
			}
			err = errors.New("you are not authorized to use this tool on this host")
			log.Error().Err(err).Str("file", cvmfsConfFile.Name()).Msg("Check your credentials")
			return ConfStruct{}, "", 0, nil, err
		} else {
			log.Error().Err(err).Str("file", cvmfsConfFile.Name()).Msg("Error reading file")
			return ConfStruct{}, "", 0, nil, err
		}
	}
	uidString, uidInt, groupIdMap, groupNames, err := GetUserGroupInfo()
	if err != nil {
		return ConfStruct{}, "", 0, nil, err
	}
	cvmfsConfInfo, err := buildCvmfsConfInfo(cvmfsConf, repoName, groupNames)
	if err != nil {
		return cvmfsConfInfo, "", 0, nil, err
	}
	return cvmfsConfInfo, uidString, uidInt, groupIdMap, nil
}

// Take in a path without its repo and return the path-prefix associated with it
func GetBasePathPrefix(cfg ConfStruct, path *pathlib.Path) ConfStruct {
	pathPrefix := path
	newCfg := cfg
	for len(pathPrefix.Clean().Parts()) > 0 && pathPrefix.Clean().Parts()[0] != CurrentDirectory {
		_, containsNormal := cfg.Repo.GroupConfig[pathPrefix.Clean().String()]
		_, containsLeading := cfg.Repo.GroupConfig["/"+pathPrefix.Clean().String()]
		if containsNormal {
			newCfg.PathPrefix = pathPrefix.Clean().String()
			newCfg.Repo.CurrentGroupConfig = newCfg.Repo.GroupConfig[newCfg.PathPrefix]
			return newCfg
		}
		if containsLeading {
			newCfg.PathPrefix = "/" + pathPrefix.Clean().String()
			newCfg.Repo.CurrentGroupConfig = newCfg.Repo.GroupConfig[newCfg.PathPrefix]
			return newCfg
		}
		pathPrefix = pathPrefix.Parent()
	}
	newCfg.PathPrefix = DefaultGroupPath
	if _, contains := newCfg.Repo.GroupConfig[newCfg.PathPrefix]; !contains {
		GiveNoPermDefault(newCfg, newCfg.PathPrefix)
	}
	newCfg.Repo.CurrentGroupConfig = newCfg.Repo.GroupConfig[newCfg.PathPrefix]
	log.Debug().Str("pathPrefix", newCfg.PathPrefix).Msg("pathPrefix")
	return newCfg
}

// Return a new configuration based on a configuration and new path.
// Note: This function expects that the base path prefix was already found using the function above
func PrefixContext(cfg ConfStruct, path *pathlib.Path) ConfStruct {
	_, containsNormal := cfg.Repo.GroupConfig[path.Clean().String()]
	_, containsLeading := cfg.Repo.GroupConfig["/"+path.Clean().String()]
	if containsNormal {
		newCfg := cfg
		newCfg.PathPrefix = path.Clean().String()
		newCfg.Repo.CurrentGroupConfig = newCfg.Repo.GroupConfig[newCfg.PathPrefix]
		return newCfg
	}
	if containsLeading {
		newCfg := cfg
		newCfg.PathPrefix = "/" + path.Clean().String()
		newCfg.Repo.CurrentGroupConfig = newCfg.Repo.GroupConfig[newCfg.PathPrefix]
		return newCfg
	}
	return cfg
}

// Setup the configuration structure with the default flag, pointing that to a config with no permissions
func GiveNoPermDefault(cfg ConfStruct, path string) {
	log.Error().Msg("No matching group configuration and no DEFAULT section. Giving no permissions.")
	noPermissions := ConfUserStruct{
		AllowUpload:      false,
		AllowPurge:       false,
		AllowDelete:      false,
		User:             0,
		Group:            0,
		FileMode:         0000,
		DirectoryMode:    0000,
		CheckPermissions: true,
		AllowAclFlag:     false,
	}
	cfg.Repo.GroupConfig[path] = noPermissions
}

func IsAlternateBucketPath(cfg ConfStruct, path *pathlib.Path) bool {
	log.Debug().Str("Path", path.Clean().String()).Msg("Checking path for alternate bucket")
	for _, bucketPath := range cfg.Repo.AlternateBucketPaths {
		if _, err := path.RelativeTo(bucketPath); err == nil {
			log.Debug().Str("Path", path.Clean().String()).Msg("Using alternate bucket")
			return true
		}
	}
	return false
}
