package common

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yl2chen/cidranger"
)

func TestBasicDefender(t *testing.T) {
	bl := HostListFile{
		IPAddresses:  []string{"172.16.1.1", "172.16.1.2"},
		CIDRNetworks: []string{"10.8.0.0/24"},
	}
	sl := HostListFile{
		IPAddresses:  []string{"172.16.1.3", "172.16.1.4"},
		CIDRNetworks: []string{"192.168.8.0/24"},
	}
	blFile := filepath.Join(os.TempDir(), "bl.json")
	slFile := filepath.Join(os.TempDir(), "sl.json")

	data, err := json.Marshal(bl)
	assert.NoError(t, err)

	err = os.WriteFile(blFile, data, os.ModePerm)
	assert.NoError(t, err)

	data, err = json.Marshal(sl)
	assert.NoError(t, err)

	err = os.WriteFile(slFile, data, os.ModePerm)
	assert.NoError(t, err)

	config := &DefenderConfig{
		Enabled:            true,
		BanTime:            10,
		BanTimeIncrement:   2,
		Threshold:          5,
		ScoreInvalid:       2,
		ScoreValid:         1,
		ScoreLimitExceeded: 3,
		ObservationTime:    15,
		EntriesSoftLimit:   1,
		EntriesHardLimit:   2,
		SafeListFile:       "slFile",
		BlockListFile:      "blFile",
	}

	_, err = newInMemoryDefender(config)
	assert.Error(t, err)
	config.BlockListFile = blFile
	_, err = newInMemoryDefender(config)
	assert.Error(t, err)
	config.SafeListFile = slFile
	d, err := newInMemoryDefender(config)
	assert.NoError(t, err)

	defender := d.(*memoryDefender)
	assert.True(t, defender.IsBanned("172.16.1.1"))
	assert.False(t, defender.IsBanned("172.16.1.10"))
	assert.False(t, defender.IsBanned("10.8.2.3"))
	assert.True(t, defender.IsBanned("10.8.0.3"))
	assert.False(t, defender.IsBanned("invalid ip"))
	assert.Equal(t, 0, defender.countBanned())
	assert.Equal(t, 0, defender.countHosts())
	assert.Len(t, defender.GetHosts(), 0)
	_, err = defender.GetHost("10.8.0.4")
	assert.Error(t, err)

	defender.AddEvent("172.16.1.4", HostEventLoginFailed)
	defender.AddEvent("192.168.8.4", HostEventUserNotFound)
	defender.AddEvent("172.16.1.3", HostEventLimitExceeded)
	assert.Equal(t, 0, defender.countHosts())

	testIP := "12.34.56.78"
	defender.AddEvent(testIP, HostEventLoginFailed)
	assert.Equal(t, 1, defender.countHosts())
	assert.Equal(t, 0, defender.countBanned())
	assert.Equal(t, 1, defender.GetScore(testIP))
	if assert.Len(t, defender.GetHosts(), 1) {
		assert.Equal(t, 1, defender.GetHosts()[0].Score)
		assert.True(t, defender.GetHosts()[0].BanTime.IsZero())
		assert.Empty(t, defender.GetHosts()[0].GetBanTime())
	}
	host, err := defender.GetHost(testIP)
	assert.NoError(t, err)
	assert.Equal(t, 1, host.Score)
	assert.Empty(t, host.GetBanTime())
	assert.Nil(t, defender.GetBanTime(testIP))
	defender.AddEvent(testIP, HostEventLimitExceeded)
	assert.Equal(t, 1, defender.countHosts())
	assert.Equal(t, 0, defender.countBanned())
	assert.Equal(t, 4, defender.GetScore(testIP))
	if assert.Len(t, defender.GetHosts(), 1) {
		assert.Equal(t, 4, defender.GetHosts()[0].Score)
	}
	defender.AddEvent(testIP, HostEventNoLoginTried)
	defender.AddEvent(testIP, HostEventNoLoginTried)
	assert.Equal(t, 0, defender.countHosts())
	assert.Equal(t, 1, defender.countBanned())
	assert.Equal(t, 0, defender.GetScore(testIP))
	assert.NotNil(t, defender.GetBanTime(testIP))
	if assert.Len(t, defender.GetHosts(), 1) {
		assert.Equal(t, 0, defender.GetHosts()[0].Score)
		assert.False(t, defender.GetHosts()[0].BanTime.IsZero())
		assert.NotEmpty(t, defender.GetHosts()[0].GetBanTime())
		assert.Equal(t, hex.EncodeToString([]byte(testIP)), defender.GetHosts()[0].GetID())
	}
	host, err = defender.GetHost(testIP)
	assert.NoError(t, err)
	assert.Equal(t, 0, host.Score)
	assert.NotEmpty(t, host.GetBanTime())

	// now test cleanup, testIP is already banned
	testIP1 := "12.34.56.79"
	testIP2 := "12.34.56.80"
	testIP3 := "12.34.56.81"

	defender.AddEvent(testIP1, HostEventNoLoginTried)
	defender.AddEvent(testIP2, HostEventNoLoginTried)
	assert.Equal(t, 2, defender.countHosts())
	time.Sleep(20 * time.Millisecond)
	defender.AddEvent(testIP3, HostEventNoLoginTried)
	assert.Equal(t, defender.config.EntriesSoftLimit, defender.countHosts())
	// testIP1 and testIP2 should be removed
	assert.Equal(t, defender.config.EntriesSoftLimit, defender.countHosts())
	assert.Equal(t, 0, defender.GetScore(testIP1))
	assert.Equal(t, 0, defender.GetScore(testIP2))
	assert.Equal(t, 2, defender.GetScore(testIP3))

	defender.AddEvent(testIP3, HostEventNoLoginTried)
	defender.AddEvent(testIP3, HostEventNoLoginTried)
	// IP3 is now banned
	assert.NotNil(t, defender.GetBanTime(testIP3))
	assert.Equal(t, 0, defender.countHosts())

	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 3; i++ {
		defender.AddEvent(testIP1, HostEventNoLoginTried)
	}
	assert.Equal(t, 0, defender.countHosts())
	assert.Equal(t, config.EntriesSoftLimit, defender.countBanned())
	assert.Nil(t, defender.GetBanTime(testIP))
	assert.Nil(t, defender.GetBanTime(testIP3))
	assert.NotNil(t, defender.GetBanTime(testIP1))

	for i := 0; i < 3; i++ {
		defender.AddEvent(testIP, HostEventNoLoginTried)
		time.Sleep(10 * time.Millisecond)
		defender.AddEvent(testIP3, HostEventNoLoginTried)
	}
	assert.Equal(t, 0, defender.countHosts())
	assert.Equal(t, defender.config.EntriesSoftLimit, defender.countBanned())

	banTime := defender.GetBanTime(testIP3)
	if assert.NotNil(t, banTime) {
		assert.True(t, defender.IsBanned(testIP3))
		// ban time should increase
		newBanTime := defender.GetBanTime(testIP3)
		assert.True(t, newBanTime.After(*banTime))
	}

	assert.True(t, defender.DeleteHost(testIP3))
	assert.False(t, defender.DeleteHost(testIP3))

	err = os.Remove(slFile)
	assert.NoError(t, err)
	err = os.Remove(blFile)
	assert.NoError(t, err)
}

func TestLoadHostListFromFile(t *testing.T) {
	_, err := loadHostListFromFile(".")
	assert.Error(t, err)

	hostsFilePath := filepath.Join(os.TempDir(), "hostfile")
	content := make([]byte, 1048576*6)
	_, err = rand.Read(content)
	assert.NoError(t, err)

	err = os.WriteFile(hostsFilePath, content, os.ModePerm)
	assert.NoError(t, err)

	_, err = loadHostListFromFile(hostsFilePath)
	assert.Error(t, err)

	hl := HostListFile{
		IPAddresses:  []string{},
		CIDRNetworks: []string{},
	}

	asJSON, err := json.Marshal(hl)
	assert.NoError(t, err)
	err = os.WriteFile(hostsFilePath, asJSON, os.ModePerm)
	assert.NoError(t, err)

	hostList, err := loadHostListFromFile(hostsFilePath)
	assert.NoError(t, err)
	assert.Nil(t, hostList)

	hl.IPAddresses = append(hl.IPAddresses, "invalidip")
	asJSON, err = json.Marshal(hl)
	assert.NoError(t, err)
	err = os.WriteFile(hostsFilePath, asJSON, os.ModePerm)
	assert.NoError(t, err)

	hostList, err = loadHostListFromFile(hostsFilePath)
	assert.NoError(t, err)
	assert.Len(t, hostList.IPAddresses, 0)

	hl.IPAddresses = nil
	hl.CIDRNetworks = append(hl.CIDRNetworks, "invalid net")

	asJSON, err = json.Marshal(hl)
	assert.NoError(t, err)
	err = os.WriteFile(hostsFilePath, asJSON, os.ModePerm)
	assert.NoError(t, err)

	hostList, err = loadHostListFromFile(hostsFilePath)
	assert.NoError(t, err)
	assert.NotNil(t, hostList)
	assert.Len(t, hostList.IPAddresses, 0)
	assert.Equal(t, 0, hostList.Ranges.Len())

	if runtime.GOOS != "windows" {
		err = os.Chmod(hostsFilePath, 0111)
		assert.NoError(t, err)

		_, err = loadHostListFromFile(hostsFilePath)
		assert.Error(t, err)

		err = os.Chmod(hostsFilePath, 0644)
		assert.NoError(t, err)
	}

	err = os.WriteFile(hostsFilePath, []byte("non json content"), os.ModePerm)
	assert.NoError(t, err)
	_, err = loadHostListFromFile(hostsFilePath)
	assert.Error(t, err)

	err = os.Remove(hostsFilePath)
	assert.NoError(t, err)
}

func TestDefenderCleanup(t *testing.T) {
	d := memoryDefender{
		banned: make(map[string]time.Time),
		hosts:  make(map[string]hostScore),
		config: &DefenderConfig{
			ObservationTime:  1,
			EntriesSoftLimit: 2,
			EntriesHardLimit: 3,
		},
	}

	d.banned["1.1.1.1"] = time.Now().Add(-24 * time.Hour)
	d.banned["1.1.1.2"] = time.Now().Add(-24 * time.Hour)
	d.banned["1.1.1.3"] = time.Now().Add(-24 * time.Hour)
	d.banned["1.1.1.4"] = time.Now().Add(-24 * time.Hour)

	d.cleanupBanned()
	assert.Equal(t, 0, d.countBanned())

	d.banned["2.2.2.2"] = time.Now().Add(2 * time.Minute)
	d.banned["2.2.2.3"] = time.Now().Add(1 * time.Minute)
	d.banned["2.2.2.4"] = time.Now().Add(3 * time.Minute)
	d.banned["2.2.2.5"] = time.Now().Add(4 * time.Minute)

	d.cleanupBanned()
	assert.Equal(t, d.config.EntriesSoftLimit, d.countBanned())
	assert.Nil(t, d.GetBanTime("2.2.2.3"))

	d.hosts["3.3.3.3"] = hostScore{
		TotalScore: 0,
		Events: []hostEvent{
			{
				dateTime: time.Now().Add(-5 * time.Minute),
				score:    1,
			},
			{
				dateTime: time.Now().Add(-3 * time.Minute),
				score:    1,
			},
			{
				dateTime: time.Now(),
				score:    1,
			},
		},
	}
	d.hosts["3.3.3.4"] = hostScore{
		TotalScore: 1,
		Events: []hostEvent{
			{
				dateTime: time.Now().Add(-3 * time.Minute),
				score:    1,
			},
		},
	}
	d.hosts["3.3.3.5"] = hostScore{
		TotalScore: 1,
		Events: []hostEvent{
			{
				dateTime: time.Now().Add(-2 * time.Minute),
				score:    1,
			},
		},
	}
	d.hosts["3.3.3.6"] = hostScore{
		TotalScore: 1,
		Events: []hostEvent{
			{
				dateTime: time.Now().Add(-1 * time.Minute),
				score:    1,
			},
		},
	}

	assert.Equal(t, 1, d.GetScore("3.3.3.3"))

	d.cleanupHosts()
	assert.Equal(t, d.config.EntriesSoftLimit, d.countHosts())
	assert.Equal(t, 0, d.GetScore("3.3.3.4"))
}

func TestDefenderConfig(t *testing.T) {
	c := DefenderConfig{}
	err := c.validate()
	require.NoError(t, err)

	c.Enabled = true
	c.Threshold = 10
	c.ScoreInvalid = 10
	err = c.validate()
	require.Error(t, err)

	c.ScoreInvalid = 2
	c.ScoreLimitExceeded = 10
	err = c.validate()
	require.Error(t, err)

	c.ScoreLimitExceeded = 2
	c.ScoreValid = 10
	err = c.validate()
	require.Error(t, err)

	c.ScoreValid = 1
	c.BanTime = 0
	err = c.validate()
	require.Error(t, err)

	c.BanTime = 30
	c.BanTimeIncrement = 0
	err = c.validate()
	require.Error(t, err)

	c.BanTimeIncrement = 50
	c.ObservationTime = 0
	err = c.validate()
	require.Error(t, err)

	c.ObservationTime = 30
	err = c.validate()
	require.Error(t, err)

	c.EntriesSoftLimit = 10
	err = c.validate()
	require.Error(t, err)

	c.EntriesHardLimit = 10
	err = c.validate()
	require.Error(t, err)

	c.EntriesHardLimit = 20
	err = c.validate()
	require.NoError(t, err)
}

func BenchmarkDefenderBannedSearch(b *testing.B) {
	d := getDefenderForBench()

	ip, ipnet, err := net.ParseCIDR("10.8.0.0/12") // 1048574 ip addresses
	if err != nil {
		panic(err)
	}

	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		d.banned[ip.String()] = time.Now().Add(10 * time.Minute)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.IsBanned("192.168.1.1")
	}
}

func BenchmarkCleanup(b *testing.B) {
	d := getDefenderForBench()

	ip, ipnet, err := net.ParseCIDR("192.168.4.0/24")
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
			d.AddEvent(ip.String(), HostEventLoginFailed)
			if d.countHosts() > d.config.EntriesHardLimit {
				panic("too many hosts")
			}
			if d.countBanned() > d.config.EntriesSoftLimit {
				panic("too many ip banned")
			}
		}
	}
}

func BenchmarkDefenderBannedSearchWithBlockList(b *testing.B) {
	d := getDefenderForBench()

	d.blockList = &HostList{
		IPAddresses: make(map[string]bool),
		Ranges:      cidranger.NewPCTrieRanger(),
	}

	ip, ipnet, err := net.ParseCIDR("129.8.0.0/12") // 1048574 ip addresses
	if err != nil {
		panic(err)
	}

	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		d.banned[ip.String()] = time.Now().Add(10 * time.Minute)
		d.blockList.IPAddresses[ip.String()] = true
	}

	for i := 0; i < 255; i++ {
		cidr := fmt.Sprintf("10.8.%v.1/24", i)
		_, network, _ := net.ParseCIDR(cidr)
		if err := d.blockList.Ranges.Insert(cidranger.NewBasicRangerEntry(*network)); err != nil {
			panic(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.IsBanned("192.168.1.1")
	}
}

func BenchmarkHostListSearch(b *testing.B) {
	hostlist := &HostList{
		IPAddresses: make(map[string]bool),
		Ranges:      cidranger.NewPCTrieRanger(),
	}

	ip, ipnet, _ := net.ParseCIDR("172.16.0.0/16")

	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		hostlist.IPAddresses[ip.String()] = true
	}

	for i := 0; i < 255; i++ {
		cidr := fmt.Sprintf("10.8.%v.1/24", i)
		_, network, _ := net.ParseCIDR(cidr)
		if err := hostlist.Ranges.Insert(cidranger.NewBasicRangerEntry(*network)); err != nil {
			panic(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if hostlist.isListed("192.167.1.2") {
			panic("should not be listed")
		}
	}
}

func BenchmarkCIDRanger(b *testing.B) {
	ranger := cidranger.NewPCTrieRanger()
	for i := 0; i < 255; i++ {
		cidr := fmt.Sprintf("192.168.%v.1/24", i)
		_, network, _ := net.ParseCIDR(cidr)
		if err := ranger.Insert(cidranger.NewBasicRangerEntry(*network)); err != nil {
			panic(err)
		}
	}

	ipToMatch := net.ParseIP("192.167.1.2")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ranger.Contains(ipToMatch); err != nil {
			panic(err)
		}
	}
}

func BenchmarkNetContains(b *testing.B) {
	var nets []*net.IPNet
	for i := 0; i < 255; i++ {
		cidr := fmt.Sprintf("192.168.%v.1/24", i)
		_, network, _ := net.ParseCIDR(cidr)
		nets = append(nets, network)
	}

	ipToMatch := net.ParseIP("192.167.1.1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, n := range nets {
			n.Contains(ipToMatch)
		}
	}
}

func getDefenderForBench() *memoryDefender {
	config := &DefenderConfig{
		Enabled:          true,
		BanTime:          30,
		BanTimeIncrement: 50,
		Threshold:        10,
		ScoreInvalid:     2,
		ScoreValid:       2,
		ObservationTime:  30,
		EntriesSoftLimit: 50,
		EntriesHardLimit: 100,
	}
	return &memoryDefender{
		config: config,
		hosts:  make(map[string]hostScore),
		banned: make(map[string]time.Time),
	}
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}
