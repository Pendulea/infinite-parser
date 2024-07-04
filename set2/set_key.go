package set2

import pcommon "github.com/pendulea/pendule-common"

func (set *Set) getAddressKey(assetKey [2]byte) []byte {
	return append([]byte(string("key")), assetKey[:]...)
}

func (set *Set) getLastUsedAssetKey() []byte {
	return []byte("last_key")
}

func (set *Set) getAssetKey(address pcommon.AssetAddress) []byte {
	return []byte(address)
}

func (set *Set) getPricesKey() []byte {
	return []byte("prices")
}
