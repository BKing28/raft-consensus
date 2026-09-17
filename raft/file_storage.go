package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type FileStorage struct {
	dir      string
	metaPath string
	logPath  string
	snapPath string
}

type stateMeta struct {
	CurrentTerm int    `json:"currentTerm"`
	VotedFor    NodeID `json:"votedFor"`
}

type snapshotFile struct {
	Index int
	Term  int
	Data  []byte
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileStorage{
		dir:      dir,
		metaPath: filepath.Join(dir, "meta.json"),
		logPath:  filepath.Join(dir, "log.json"),
		snapPath: filepath.Join(dir, "snapshot.json"),
	}, nil
}

func (fs *FileStorage) GetState() (int, NodeID, error) {
	data, err := os.ReadFile(fs.metaPath)
	if os.IsNotExist(err) {
		return 0, NullVote, nil
	}
	if err != nil {
		return 0, NullVote, err
	}
	var m stateMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, NullVote, err
	}
	return m.CurrentTerm, m.VotedFor, nil
}

func (fs *FileStorage) SetState(currentTerm int, votedFor NodeID) error {
	return fs.writeJSON(fs.metaPath, stateMeta{currentTerm, votedFor})
}

func (fs *FileStorage) GetLogRange(lo, hi int) ([]LogEntry, error) {
	snapIndex, entries, err := fs.readLog()
	if err != nil {
		return nil, err
	}
	if lo <= snapIndex || hi > snapIndex+len(entries) {
		return nil, fmt.Errorf("log range [%d,%d] out of bounds (len=%d)", lo, hi, len(entries))
	}
	result := make([]LogEntry, hi-lo+1)
	copy(result, entries[lo-1-snapIndex:hi-snapIndex])
	return result, nil
}

func (fs *FileStorage) LastIndex() (int, error) {
	snapIndex, entries, err := fs.readLog()
	if err != nil {
		return 0, err
	}
	return snapIndex + len(entries), nil
}

func (fs *FileStorage) AppendLogs(entries []LogEntry) error {
	_, existing, err := fs.readLog()
	if err != nil {
		return err
	}
	existing = append(existing, entries...)
	return fs.writeJSON(fs.logPath, existing)
}

func (fs *FileStorage) TruncateFrom(index int) error {
	snapIndex, entries, err := fs.readLog()
	if err != nil {
		return err
	}
	if index-1-snapIndex < len(entries) {
		entries = entries[:index-1-snapIndex]
	}
	return fs.writeJSON(fs.logPath, entries)
}

func (fs *FileStorage) SaveSnapshot(index, term int, data []byte) error {
	snapIndex, entries, err := fs.readLog()
	if err != nil {
		return err
	}
	if err := fs.writeJSON(fs.snapPath, snapshotFile{index, term, data}); err != nil {
		return err
	}
	return fs.writeJSON(fs.logPath, entries[min(index-snapIndex, len(entries)):])
}

func (fs *FileStorage) LoadSnapshot() (int, int, []byte, error) {
	data, err := os.ReadFile(fs.snapPath)
	if os.IsNotExist(err) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}
	var s snapshotFile
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, 0, nil, err
	}
	return s.Index, s.Term, s.Data, nil
}

// Entries at or below the snapshot index are skipped, so a crash between the snapshot and log writes is harmless.
func (fs *FileStorage) readLog() (int, []LogEntry, error) {
	snapIndex, _, _, err := fs.LoadSnapshot()
	if err != nil {
		return 0, nil, err
	}
	data, err := os.ReadFile(fs.logPath)
	if os.IsNotExist(err) {
		return snapIndex, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	var entries []LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return 0, nil, err
	}
	for len(entries) > 0 && entries[0].Index <= snapIndex {
		entries = entries[1:]
	}
	return snapIndex, entries, nil
}

// Writes JSON to file via a temp file + rename for atomicity, then fsyncs the directory.
func (fs *FileStorage) writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}

	if err := syncFile(tmp); err != nil {
		return err
	}

	if err := os.Rename(tmp, path); err != nil {
		return err
	}

	return syncDir(fs.dir)
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
