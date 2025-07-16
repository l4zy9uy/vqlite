package table

import (
	"os"
	"testing"
	"vqlite/column"
	"vqlite/pager"
)

// TestBTreeDelete_Basic tests basic deletion functionality
func TestBTreeDelete_Basic(t *testing.T) {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "btree_delete_test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Open pager and create B-tree
	pg, err := pager.OpenPager(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "name", Type: column.ColumnTypeText, MaxLength: 16},
	}

	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("Failed to build table meta: %v", err)
	}

	bt, err := NewBTree(pg, meta)
	if err != nil {
		t.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert test data
	testData := []struct {
		key  uint32
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "David"},
		{5, "Eve"},
	}

	for _, data := range testData {
		row := Row{data.key, data.name}
		if err := bt.Insert(data.key, row); err != nil {
			t.Fatalf("Failed to insert key %d: %v", data.key, err)
		}
	}

	// Test deletion of existing keys
	for _, data := range testData {
		found, err := bt.Delete(data.key)
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", data.key, err)
		}
		if !found {
			t.Errorf("Expected to find key %d for deletion", data.key)
		}

		// Verify key is no longer in tree
		_, exists, err := bt.Search(data.key)
		if err != nil {
			t.Fatalf("Failed to search for deleted key %d: %v", data.key, err)
		}
		if exists {
			t.Errorf("Key %d should not exist after deletion", data.key)
		}
	}

	// Test deletion of non-existent key
	found, err := bt.Delete(999)
	if err != nil {
		t.Fatalf("Failed to attempt deletion of non-existent key: %v", err)
	}
	if found {
		t.Errorf("Expected not to find non-existent key 999")
	}
}

// TestBTreeDelete_PartialDeletion tests deleting some keys while others remain
func TestBTreeDelete_PartialDeletion(t *testing.T) {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "btree_delete_partial_test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Open pager and create B-tree
	pg, err := pager.OpenPager(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
	}

	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("Failed to build table meta: %v", err)
	}

	bt, err := NewBTree(pg, meta)
	if err != nil {
		t.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert data
	keys := []uint32{10, 20, 30, 40, 50}
	for _, key := range keys {
		row := Row{key}
		if err := bt.Insert(key, row); err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Delete some keys
	keysToDelete := []uint32{20, 40}
	for _, key := range keysToDelete {
		found, err := bt.Delete(key)
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", key, err)
		}
		if !found {
			t.Errorf("Expected to find key %d for deletion", key)
		}
	}

	// Verify remaining keys still exist
	remainingKeys := []uint32{10, 30, 50}
	for _, key := range remainingKeys {
		row, exists, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for remaining key %d: %v", key, err)
		}
		if !exists {
			t.Errorf("Expected key %d to still exist", key)
		}
		if row[0].(uint32) != key {
			t.Errorf("Expected row with key %d, got %d", key, row[0].(uint32))
		}
	}

	// Verify deleted keys no longer exist
	for _, key := range keysToDelete {
		_, exists, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for deleted key %d: %v", key, err)
		}
		if exists {
			t.Errorf("Key %d should not exist after deletion", key)
		}
	}
}

// TestBTreeDelete_EmptyTree tests deletion from empty tree
func TestBTreeDelete_EmptyTree(t *testing.T) {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "btree_delete_empty_test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Open pager and create B-tree
	pg, err := pager.OpenPager(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
	}

	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("Failed to build table meta: %v", err)
	}

	bt, err := NewBTree(pg, meta)
	if err != nil {
		t.Fatalf("Failed to create B-tree: %v", err)
	}

	// Try to delete from empty tree
	found, err := bt.Delete(42)
	if err != nil {
		t.Fatalf("Failed to delete from empty tree: %v", err)
	}
	if found {
		t.Errorf("Expected not to find key in empty tree")
	}
}
