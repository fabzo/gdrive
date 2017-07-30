package drive

import (
	"fmt"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"io"
	"time"
)

type FixSyncHierarchyArgs struct {
	Out              io.Writer
	Progress         io.Writer
	RootId           string
	DryRun           bool
}

func (self *Drive) FixSyncHierarchy(args FixSyncHierarchyArgs) error {
	fmt.Fprintln(args.Out, "Starting fixing the sync hierarchy...")
	if args.DryRun {
		fmt.Fprintln(args.Out, "This is a dry run!")
	}
	started := time.Now()

	// Find sync root dir
	fmt.Fprintln(args.Out, "Searching for the given root id...")
	rootDir, err := self.findSyncRoot(args.RootId)
	if err != nil {
		return err
	}

	// Collect all remote files
	fmt.Fprintln(args.Out, "Collecting a list of all files in the drive...")
	listArgs := listAllFilesArgs{
		query:     "trashed = false and 'me' in owners",
		fields:    []googleapi.Field{"nextPageToken", "files(id,name,parents,md5Checksum,mimeType,size,modifiedTime,appProperties)"},
		sortOrder: "",
	}
	files, err := self.listAllFiles(listArgs)
	if err != nil {
		return fmt.Errorf("Failed listing files: %s", err)
	}

	fmt.Fprintf(args.Out, "Found %d files. Filtering files in the sync dir hierarchy...\n", len(files))
	syncDirFiles, nonSyncDirFiles := self.filterSyncDirFiles(rootDir, files)

	fields := []googleapi.Field{"id", "name", "mimeType", "appProperties"}
	for _, file := range syncDirFiles {
		syncRootId := file.AppProperties["syncRootId"]
		if syncRootId == rootDir.Id  || file.Id == rootDir.Id {
			continue
		}
		fmt.Fprintf(args.Out, "Updating syncRootId of %s [%s]. Is '%s', but should be '%s'\n", file.Name, file.Id, syncRootId, args.RootId)

		// Update directory with syncRootId property
		dstFile := &drive.File{
			AppProperties: map[string]string{"syncRootId": args.RootId},
		}

		if !args.DryRun {
			file, err = self.service.Files.Update(file.Id, dstFile).Fields(fields...).Do()
			if err != nil {
				return fmt.Errorf("Failed to update syncRootId of directory: %s", err)
			}
		}
	}

	for _, file := range nonSyncDirFiles {
		syncRootId := ""
		ok := false
		if syncRootId, ok = file.AppProperties["syncRootId"]; ok {
			if syncRootId == "" {
				continue
			}
		}

		// Update directory with syncRootId property
		dstFile := &drive.File{
			AppProperties: map[string]string{"syncRootId": ""},
		}

		fmt.Fprintf(args.Out, "Updating syncRootId of %s [%s]. Is '%s', but should be non existant or empty\n", file.Name, file.Id, syncRootId)
		if !args.DryRun {
			file, err = self.service.Files.Update(file.Id, dstFile).Fields(fields...).Do()
			if err != nil {
				return fmt.Errorf("Failed to update syncRootId of directory: %s", err)
			}
		}
	}

	fmt.Fprintf(args.Out, "Sync finished in %s\n", time.Since(started))

	return nil
}

func (self *Drive) filterSyncDirFiles(rootDir *drive.File, files []*drive.File) ([]*drive.File, []*drive.File){
	// A map of all files / directories that belong in the rootDir hierarchy
	syncDirFilesMap := map[string]*drive.File{
		rootDir.Id: rootDir,
	}

	// Files that have yet to be processed
	filesToProcess := files

	foundSyncDirFile := true
	for foundSyncDirFile {
		foundSyncDirFile = false

		for idx, file := range filesToProcess {
			if _, ok := syncDirFilesMap[file.Parents[0]]; ok {
				foundSyncDirFile = true

				syncDirFilesMap[file.Id] = file
				filesToProcess = append(filesToProcess[:idx], filesToProcess[idx+1:]...)

				break
			} else if file.Id == rootDir.Id {
				filesToProcess = append(filesToProcess[:idx], filesToProcess[idx+1:]...)
			}
		}
	}

	syncDirFiles := []*drive.File{}
	for _, file := range syncDirFilesMap {
		syncDirFiles = append(syncDirFiles, file)
	}

	return syncDirFiles, filesToProcess
}

func (self *Drive) findSyncRoot(rootId string) (*drive.File, error) {
	fields := []googleapi.Field{"id", "name", "mimeType", "appProperties"}
	f, err := self.service.Files.Get(rootId).Fields(fields...).Do()
	if err != nil {
		return nil, fmt.Errorf("Failed to find root dir: %s", err)
	}

	// Ensure file is a directory
	if !isDir(f) {
		return nil, fmt.Errorf("Provided root id is not a directory")
	}

	// Return directory if syncRoot property is already set
	if _, ok := f.AppProperties["syncRoot"]; ok {
		return f, nil
	}

	return nil, fmt.Errorf("Root dir with id %s is not a sync root", rootId)
}
