package storage

type StorageManager struct {
	collections map[string]*Collection
}

func InitDbManager() StorageManager {

	dbManager := StorageManager{
		collections: make(map[string]*Collection),
	}
	return dbManager
}

func (db *StorageManager) Shutdown() {
	for _, collection := range db.collections {
		collection.Close()
	}
}
