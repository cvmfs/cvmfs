package pkg

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	"github.com/rs/zerolog/log"
)

func cleanAclString(contents string) (string, error) {
	// This is only expected to work on linux, so not worrying about \r
	cleanedLines := []string{}
	lines := strings.Split(contents, LineSeparator)
	for _, line := range lines {
		trimLine := strings.TrimSpace(line)
		if len(trimLine) > 0 && trimLine[0] != FaclCommentChar && (len(trimLine) < len(FaclDefaultStr) || trimLine[0:len(FaclDefaultStr)] != FaclDefaultStr) {
			if strings.Contains(trimLine, string(FaclCommentChar)) {
				err := fmt.Errorf("Inline comment, please remove")
				log.Error().Err(err).Str("Erroring line", line).Msg("Error with facl file")
				return "", err
			}
			cleanedLines = append(cleanedLines, trimLine)
		} else {
			log.Debug().Str("Line Removed", line).Msg("Removing line (comment or default)")
		}
	}
	return strings.Join(cleanedLines, LineSeparator), nil
}

func GetAclFromFile(file string) (string, error) {
	contents, err := os.ReadFile(file)
	if err != nil {
		log.Error().Err(err).Msg("Error reading facl file")
		return "", err
	}
	cleanedContents, err := cleanAclString(string(contents))
	if err != nil {
		log.Error().Err(err).Msg("Error with cleaning acl string")
		return "", err
	}
	return cleanedContents, nil
}

func getAclIdentifiers(aclEntry *acl.Entry) (acl.Tag, string, error) {
	tag, err := aclEntry.GetTag()
	if err != nil {
		return 0, "", err
	}
	if tag == acl.TagUser {
		qual, err := aclEntry.GetQualifier()
		if err != nil {
			log.Error().Err(err).Msg("Error getting submitted qualifier")
			return 0, "", err
		}
		usr, err := user.LookupId(strconv.Itoa(qual))
		if err != nil {
			log.Error().Err(err).Int("User", qual).Msg("Error getting submitted user")
			return 0, "", err
		}
		return tag, usr.Uid, nil
	}
	if tag == acl.TagGroup {
		qual, err := aclEntry.GetQualifier()
		if err != nil {
			log.Error().Err(err).Msg("Error getting submitted qualifier")
			return 0, "", err
		}
		grp, err := user.LookupGroupId(strconv.Itoa(qual))
		if err != nil {
			log.Error().Err(err).Int("Group", qual).Msg("Error getting submitted group")
			return 0, "", err
		}
		return tag, grp.Gid, nil
	}
	if tag == acl.TagOther || tag == acl.TagUserObj || tag == acl.TagGroupObj {
		return tag, "", nil
	}
	err = fmt.Errorf("Unmodifiable tag")
	return tag, "", err
}

func FindAclEntry(aclEntry *acl.Entry, dirAcl *acl.ACL) (*acl.Entry, error) {
	delTag, delQual, err := getAclIdentifiers(aclEntry)
	if err != nil {
		return nil, err
	}
	for exEntry := dirAcl.FirstEntry(); exEntry != nil; exEntry = dirAcl.NextEntry() {
		exTag, exQual, err := getAclIdentifiers(exEntry)
		if err != nil {
			continue
		}

		if delTag == exTag && delQual == exQual {
			return exEntry, nil
		}
	}
	return nil, nil
}
