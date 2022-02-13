package surfstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func uploadFile(filename, blockStoreAddr string, client RPCClient) error {
	fmt.Println("Uploading...")
	filepath := ConcatPath(client.BaseDir, filename)
	f, _ := os.Open(filepath)
	dataBlocks := getDataBlocks(f, client.BlockSize)
	var block Block
	var succ bool
	for _, dataBlock := range dataBlocks {
		block.BlockData = []byte(dataBlock)
		block.BlockSize = int32(len([]byte(dataBlock)))

		err := client.PutBlock(&block, blockStoreAddr, &succ)
		if err != nil {
			return err
		}
		if !succ {
			return errors.New("PutBlock failed")
		}
	}

	return nil
}

func updateLocalIndex(filename string, remoteFileMetaData *FileMetaData, newRecord *FileMetaData) {
	newRecord.Filename = filename
	newRecord.BlockHashList = remoteFileMetaData.GetBlockHashList()
	newRecord.Version = remoteFileMetaData.GetVersion()
}

func downloadFile(filename, blockStoreAddr string, hashList []string, client RPCClient) {
	fmt.Println("Downloading...")
	filepath := ConcatPath(client.BaseDir, filename)
	file, _ := os.Create(filepath)
	defer file.Close()

	consolidatedData := make([]string, 0)
	var block Block
	for _, hash := range hashList {
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			log.Fatal(err)
		}
		consolidatedData = append(consolidatedData, string(block.BlockData)) // Storing each block in the same variable might cause problems
	}

	file.Write([]byte(strings.Join(consolidatedData, "")))
}

func getDataBlocks(file *os.File, blockSize int) []string {
	defer file.Close()
	var blocks = make([]string, 0)

	r := bufio.NewReader(file)
	buf := make([]byte, 0, blockSize)

	for {
		n, err := io.ReadFull(r, buf[:cap(buf)])
		buf = buf[:n]
		if err != nil {
			if err == io.EOF {
				break
			}
			if err != io.ErrUnexpectedEOF {
				fmt.Fprintln(os.Stderr, err)
				break
			}
		}

		blocks = append(blocks, string(buf))
	}
	return blocks
}

func getHashList(file *os.File, blockSize int) []string {
	blocks := getDataBlocks(file, blockSize)
	var hashList = make([]string, 0)
	for _, block := range blocks {
		hashList = append(hashList, GetBlockHashString([]byte(block)))
	}

	return hashList
}

func hashListsEqual(hashList1, hashList2 []string) bool {
	if len(hashList1) != len(hashList2) {
		return false
	}
	for i := 0; i < len(hashList1); i++ {
		if hashList1[i] != hashList2[i] {
			return false
		}
	}
	return true
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// First, we update local index
	// get local file meta map
	localFileMetaMap, _ := LoadMetaFromMetaFile(client.BaseDir)

	// access all files in base dir
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	// Create a filename: fileInfo map
	fileMap := make(map[string]os.FileInfo)
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		fileMap[file.Name()] = file
	}

	// for _, file := range files {
	// if file.Name() == DEFAULT_META_FILENAME {
	// 	continue
	// }

	// 	filePath := ConcatPath(client.BaseDir, file.Name())
	// 	f, _ := os.Open(filePath)

	// 	// check if file exists in localFileMetaMap. If DNE, then its a new file, add a record
	// 	// else check if it has been updated. If it has been updated, update the record, else do nothing
	// 	if fileMetaData, exists := localFileMetaMap[file.Name()]; exists { // the file exists in local index
	// 		hashList := getHashList(f, client.BlockSize) // current file state hash list

	// 		if !areHashListsEqual(hashList, fileMetaData.GetBlockHashList()) { // the file has been updated
	// 			fileMetaData.BlockHashList = hashList
	// 			fileMetaData.Version = fileMetaData.GetVersion() + 1
	// 		}
	// 	} else { // file DNE in local index, create and add record
	// 		var newRecord FileMetaData

	// 		newRecord.Filename = file.Name()
	// 		newRecord.BlockHashList = getHashList(f, client.BlockSize)
	// 		newRecord.Version = 1

	// 		localFileMetaMap[file.Name()] = &newRecord
	// 	}
	// }

	// // check for deleted files in local index
	// for filename, fileMetaData := range localFileMetaMap {
	// 	if _, exists := fileMap[filename]; !exists {
	// 		fileMetaData.BlockHashList = []string{"0"}
	// 		fileMetaData.Version = fileMetaData.GetVersion() + 1

	// 	}
	// }

	// // update local index.txt
	// err = WriteMetaFile(localFileMetaMap, client.BaseDir)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Connect to server and download FileInfoMap
	var remoteFileMetaMap map[string]*FileMetaData
	client.GetFileInfoMap(&remoteFileMetaMap)

	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Check if file in remote index is present in local index or not
	// Note: If file exists on cloud and locally, then it must also be present in index.txt
	for filename, remoteFileMetaData := range remoteFileMetaMap {
		if _, exists := fileMap[filename]; exists { // if remote file exists locally
			f, _ := os.Open(ConcatPath(client.BaseDir, filename))
			fileHash := getHashList(f, client.BlockSize)

			if hashListsEqual(fileHash, localFileMetaMap[filename].GetBlockHashList()) { // No uncommitted local modifications
				if remoteFileMetaData.GetVersion() > localFileMetaMap[filename].GetVersion() { // Remote index version is higher than local version
					downloadFile(filename, blockStoreAddr, remoteFileMetaData.GetBlockHashList(), client)

					var modRecord FileMetaData
					updateLocalIndex(filename, remoteFileMetaData, &modRecord)

					localFileMetaMap[filename] = &modRecord
				}
			} else { // Uncommitted local modifications are present
				if remoteFileMetaData.GetVersion() > localFileMetaMap[filename].GetVersion() { // Remote index version is higher than local version
					downloadFile(filename, blockStoreAddr, remoteFileMetaData.GetBlockHashList(), client)

					var modRecord FileMetaData
					updateLocalIndex(filename, remoteFileMetaData, &modRecord)

					localFileMetaMap[filename] = &modRecord
				} else { // Remote index version is same as version in index.txt
					err = uploadFile(filename, blockStoreAddr, client)
					if err != nil {
						log.Fatal(err)
					}

					f, _ := os.Open(ConcatPath(client.BaseDir, filename))

					var modRecord FileMetaData

					modRecord.Filename = filename
					modRecord.BlockHashList = getHashList(f, client.BlockSize)
					modRecord.Version = localFileMetaMap[filename].GetVersion() + 1

					var latestVersion int32
					err = client.UpdateFile(&modRecord, &latestVersion)
					if err != nil {
						// Version error!
						downloadFile(filename, blockStoreAddr, remoteFileMetaData.GetBlockHashList(), client)
						updateLocalIndex(filename, remoteFileMetaData, &modRecord)
					}

					localFileMetaMap[filename] = &modRecord
				}
			}
		} else { // if remote file DNE locally, download blocks and reconstitute file
			downloadFile(filename, blockStoreAddr, remoteFileMetaData.GetBlockHashList(), client)

			var newRecord FileMetaData
			updateLocalIndex(filename, remoteFileMetaData, &newRecord)

			localFileMetaMap[filename] = &newRecord
		}
	}

	// update local index.txt
	err = WriteMetaFile(localFileMetaMap, client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	// Next, it is possible that there are new files in the local base directory that aren’t in the local index or in the remote index
	for filename := range fileMap {
		if remoteFileMetaData, exists := remoteFileMetaMap[filename]; exists {
			// TODO: Handle conflict case
			fmt.Println(remoteFileMetaData)
		} else { // In this case the file DNE remotely and also DNE in index.txt
			err = uploadFile(filename, blockStoreAddr, client)
			if err != nil {
				log.Fatal(err)
			}

			f, _ := os.Open(ConcatPath(client.BaseDir, filename))

			var newRecord FileMetaData

			newRecord.Filename = filename
			newRecord.BlockHashList = getHashList(f, client.BlockSize)
			newRecord.Version = 1

			var latestVersion int32
			err = client.UpdateFile(&newRecord, &latestVersion)
			if err != nil {
				// Version error!
				downloadFile(filename, blockStoreAddr, remoteFileMetaData.GetBlockHashList(), client)
				updateLocalIndex(filename, remoteFileMetaData, &newRecord)
			}

			localFileMetaMap[filename] = &newRecord
		}
	}

	// update local index.txt
	err = WriteMetaFile(localFileMetaMap, client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println("HI!!!!!!")
	// Check if file in local index is present in remote index or not
	// for filename, localFileMetaData := range localFileMetaMap {
	// 	if remoteFileMetaData, exists := remoteFileMetaMap[filename]; exists { // if local file exists remotely
	// 		// TODO: Handle conflict case
	// 		fmt.Println(remoteFileMetaData)
	// 	} else { // if local file DNE remotely, upload blocks
	// 		f, _ := os.Open(ConcatPath(client.BaseDir, filename))
	// 		localFileBlocks := getDataBlocks(f, client.BlockSize)
	// 		var block Block
	// 		var succ bool
	// 		for _, localFileBlock := range localFileBlocks {
	// 			block.BlockData = []byte(localFileBlock)
	// 			block.BlockSize = int32(len([]byte(localFileBlock)))
	// 			err := client.PutBlock(&block, blockStoreAddr, &succ)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}
	// 		}

	// 		// update remote index
	// 		var newRecord FileMetaData

	// 		newRecord.Filename = filename
	// 		newRecord.BlockHashList = localFileMetaData.GetBlockHashList()
	// 		newRecord.Version = localFileMetaData.GetVersion()

	// 		var latestVersion int32
	// 		client.UpdateFile(&newRecord, &latestVersion)

	// 		// remoteFileMetaMap[filename] = &newRecord		// TODO: Check if this line is required or not
	// 	}
	// }
	// fmt.Println("HI!!!!!!!!!!!")
	// panic("todo")
}
