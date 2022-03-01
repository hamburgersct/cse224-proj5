package surfstore

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// check local index file
	indexFilePath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		file, _ := os.Create(indexFilePath)
		defer file.Close()
	}
	// Open index.txt
	indFile, err := os.OpenFile(indexFilePath, os.O_APPEND|os.O_RDWR, 0777)
	if err != nil {
		log.Println(err)
	}

	localIndexFileMetaMap, allFileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Cannot load index.txt")
	}

	clientMetaMap := make(map[string]*FileMetaData)
	clientBlockMap := make(map[string]*Block)

	// Look at base directory, compute each file's hash list
	dir, err := os.Open(client.BaseDir)
	if err != nil {
		log.Println("Cannot open the directory.")
	}
	list, _ := dir.Readdirnames(0)
	for _, filename := range list {
		if filename == "index.txt" {
			continue
		}
		f, _ := os.Open(ConcatPath(client.BaseDir, filename))
		i := 0
		var newBlockHashList []string
		// Read block size bytes for each until until EOF
		for {
			buf := make([]byte, client.BlockSize)
			bytesRead, err := f.Read(buf)
			if err != nil {
				break
			}
			// Encode with sha256 and hex encoding
			var newBlock Block
			// Sum(buf...)
			newBlock.BlockData = buf[:bytesRead]
			newBlock.BlockSize = int32(bytesRead)
			encodedHash := GetBlockHashString(newBlock.BlockData)
			clientBlockMap[encodedHash] = &newBlock
			newBlockHashList = append(newBlockHashList, encodedHash)
			i++
		}

		var newFileMetaData FileMetaData
		newFileMetaData.Filename = filename
		newFileMetaData.BlockHashList = newBlockHashList
		newFileMetaData.Version = 1

		// check if a file is in local index
		fLoc, inLocalIndex := localIndexFileMetaMap[filename]
		if inLocalIndex {
			if IsBlockHashListModified(newBlockHashList, fLoc.BlockHashList) {
				newFileMetaData.Version = fLoc.Version + 1
			} else {
				newFileMetaData.Version = fLoc.Version
			}
		}

		clientMetaMap[filename] = &newFileMetaData
		_, inSet := allFileMetaMap[filename]
		if !inSet {
			allFileMetaMap[filename] = true
		}
		f.Close()
	}
	dir.Close()

	// Compare local index with remote index
	// 1. If files on remote index not in local or base dir, download those files, add to local index
	// 2. If files not in local OR remote (add to remote first, then if successful add to local)
	remoteFileInfoMap := make(map[string]*FileMetaData)
	serverErr := client.GetFileInfoMap(&remoteFileInfoMap)
	if serverErr != nil {
		log.Println("Failed to get remote index.")
	}
	for filename := range remoteFileInfoMap {
		_, inSet := allFileMetaMap[filename]
		if !inSet {
			allFileMetaMap[filename] = true
		}
	}

	// Conflict cases
	// 1. No local changes, remote version higher -> download blocks from remote and update local index and file in local base dir
	// 2. Uncomitted local changes, local index and remote index version same, update mapping on remote, then local index (no file change necessary)
	// 3. Local modifications to file (uncommited local changes), file version on remote > local index -> update local with remote version / bring local version of file up to date with server
	for fname := range allFileMetaMap {
		fBasedDir, inBasedDir := clientMetaMap[fname]
		fLocalIndex, inLocalIndex := localIndexFileMetaMap[fname]
		fServer, inServer := remoteFileInfoMap[fname]

		var newFileMetaData FileMetaData
		var latestVersion int32

		// Case: in Based dir (in or not in Local) -> Update File (get from server if err)
		// Case: not in based dir, in Local (DELETED) -> UpdateFile (get from server if err)
		// Case: Otherwise, not in EITHER -> Get from Server
		if inBasedDir || inLocalIndex {
			// changed or new file
			if inBasedDir {
				// TestSyncTwoClientsSameFile error
				// TODO: check hash differ
				newFileMetaData = *fBasedDir
				var blockStoreAddr string
				err := client.GetBlockStoreAddr(&blockStoreAddr)
				if err != nil {
					log.Println("Cannot get block store address.")
				}
				for _, blockHash := range newFileMetaData.BlockHashList {
					newBlock := clientBlockMap[blockHash]
					var succ bool
					client.PutBlock(newBlock, blockStoreAddr, &succ)
				}
			} else if inLocalIndex {
				// Deleted File
				newFileMetaData.Filename = fname
				newFileMetaData.Version = fLocalIndex.Version + 1
				newFileMetaData.BlockHashList = []string{"0"}
			}

			err = client.UpdateFile(&newFileMetaData, &latestVersion)
			// If err -> download from server
			if err != nil {
				// Check if updated version is to remove file
				if len(fServer.BlockHashList) == 1 && fServer.BlockHashList[0] == "0" {
					os.Remove(ConcatPath(client.BaseDir, fname))
				} else {
					// TODO: remote version == loc version?

					// If updated version is new content in the file
					updatedFile, _ := os.OpenFile(ConcatPath(client.BaseDir, fname), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0755)
					updatedFile.Truncate(0)
					var blockStoreAddr string
					err = client.GetBlockStoreAddr(&blockStoreAddr)
					if err != nil {
						log.Println(err)
					}
					for _, blockHash := range fServer.BlockHashList {
						var blockToGet Block
						err = client.GetBlock(blockHash, blockStoreAddr, &blockToGet)
						if err != nil {
							log.Println("Cannot get block.")
							log.Println(err)
						}
						updatedFile.Write(blockToGet.BlockData)
					}
					updatedFile.Close()
				}
				// Write to index.txt server version
				indFile.Write([]byte(fname + "," + strconv.Itoa(int(fServer.GetVersion())) + "," + strings.Join(fServer.BlockHashList, " ") + "\n"))
			} else {
				indFile.Write([]byte(fname + "," + strconv.Itoa(int(newFileMetaData.GetVersion())) + "," + strings.Join(newFileMetaData.BlockHashList, " ") + "\n"))
			}
		} else if inServer {
			// Download from Server
			if len(fServer.BlockHashList) == 1 && fServer.BlockHashList[0] == "0" {
				os.Remove(ConcatPath(client.BaseDir, fname))
			} else {
				// If updated version is new content in the file
				var blockStoreAddr string
				err = client.GetBlockStoreAddr(&blockStoreAddr)
				if err != nil {
					log.Println(err)
				}
				updatedFile, _ := os.OpenFile(ConcatPath(client.BaseDir, fname), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0755)
				updatedFile.Truncate(0)
				for _, blockHash := range fServer.BlockHashList {
					var blockToGet Block
					err = client.GetBlock(blockHash, blockStoreAddr, &blockToGet)
					if err != nil {
						log.Println("Cannot get block.")
						log.Println(err)
					}
					updatedFile.Write(blockToGet.BlockData)
				}
				updatedFile.Close()
			}
			// Write to index.txt server
			indFile.Write([]byte(fname + "," + strconv.Itoa(int(fServer.GetVersion())) + "," + strings.Join(fServer.BlockHashList, " ") + "\n"))
		}
	}

	indFile.Close()

	log.Println("\n Server Map After")
	client.GetFileInfoMap(&remoteFileInfoMap)
	PrintMetaMap(remoteFileInfoMap)
}

func IsBlockHashListModified(hashList1 []string, hashList2 []string) bool {
	res := false
	if ((hashList1 == nil) != (hashList2 == nil)) || (len(hashList1) != len(hashList2)) {
		res = true
	} else {
		for i := range hashList1 {
			if hashList1[i] != hashList2[i] {
				res = true
				break
			}
		}
	}
	return res
}
