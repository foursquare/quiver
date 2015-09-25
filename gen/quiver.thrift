namespace go gen

enum HFileServiceAction {
  fetchValuesSingle
  fetchValuesMulti
  getIterator
  fetchValuesForPrefixes
  fetchValuesForSplitKeys
}

exception HFileServiceException {
  1: optional string message
}

struct SingleHFileKeyRequest {
  1: optional string hfileName
  // Keys to look up.
  // Note: For efficiency, keys must be sorted by the client. If they are not, behavior is undefined.
  2: optional list<binary> sortedKeys
  3: optional i32 perKeyValueLimit
  4: optional bool countOnly
}

struct SingleHFileKeyResponse {
  // A map from index in the sorted_keys list to value of that key.
  // A missing index means that key had no value in the served hfile.
  1: optional map<i32, binary> values
  2: optional i32 keyCount
}

struct MultiHFileKeyResponse {
  // A map from index in the SingleHFileKeyRequest.sortedKeys list to values.
  // A missing index means that no key in the corresponding SingleHFileKeyRequest.sortedKeys is found.
  1: optional map<i32, list<binary>> values
  2: optional i32 keyCount
}

struct PrefixRequest {
  1: optional string hfileName
  2: optional list<binary> sortedKeys
}

struct PrefixResponse {
  1: optional map<binary, list<binary>> values
}

struct MultiHFileSplitKeyRequest {
  1: optional string hfileName
  2: optional list<binary> retired_sortedPrefixes
  3: optional list<binary> retired_sortedSuffixes
  4: optional list<list<binary>> splitKey
}

struct KeyToValuesResponse {
  1: optional map<binary, list<binary>> values
}

struct KeyValueItem {
  1: optional binary key
  2: optional binary value
}

struct IteratorRequest {
  1: optional string hfileName
  2: optional bool includeValues
  // lastKey and skipKeys combined informs where to continue with the next batch of iterator request
  // To continue from where previously left off, seek to the position of lastKey and then skip forward skipKeys of keys.
  3: optional binary lastKey
  4: optional i32 skipKeys
  5: optional i32 responseLimit
  6: optional binary endKey
}

struct IteratorResponse {
  1: optional list<KeyValueItem> values
  2: optional binary lastKey
  3: optional i32 skipKeys
}

struct HFileInfo {
  1: optional string name
  2: optional string path
  3: optional i64 numElements
  // The first and last keys in the union of the hfiles the server is serving from.
  4: optional binary firstKey
  5: optional binary lastKey

  // Some random keys found in the server's hfiles.
  6: optional list<binary> randomKeys
}

struct InfoRequest {
  // Number of random keys to return. Use judiciously: finding evenly-distributed random keys may require
  // a full scan on the hfile, which may (in some implementations) block other readers.
  1: optional string hfileName
  2: optional i64 numRandomKeys
}

service HFileService {

  SingleHFileKeyResponse getValuesSingle(1: SingleHFileKeyRequest req) throws (1: HFileServiceException ex);

  MultiHFileKeyResponse getValuesMulti(1: SingleHFileKeyRequest req) throws (1: HFileServiceException ex);

  PrefixResponse getValuesForPrefixes(1: PrefixRequest req) throws (1: HFileServiceException ex);

  KeyToValuesResponse getValuesMultiSplitKeys(1: MultiHFileSplitKeyRequest req) throws (1: HFileServiceException ex);

  IteratorResponse getIterator(1: IteratorRequest req) throws (1: HFileServiceException ex);

  list<HFileInfo> getInfo(1: InfoRequest req) throws (1: HFileServiceException ex);

  i32 testTimeout(1: i32 waitInMillis);
}
