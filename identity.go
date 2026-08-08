package evpanda

import "strings"

// RoamingIdentity is the OCPI roaming context for a message. PlatformID
// and PlatformName are required; TenantID and TenantName are optional but
// all-or-nothing (supply both or neither).
type RoamingIdentity struct {
	PlatformID   string
	PlatformName string
	TenantID     string
	TenantName   string
}

// ChargerIdentity is the OCPP charger context for a message. ChargerID is
// required; TenantID and TenantName are optional but all-or-nothing.
type ChargerIdentity struct {
	ChargerID  string
	TenantID   string
	TenantName string
}

func isNonEmpty(v string) bool {
	return strings.TrimSpace(v) != ""
}

// isTenantPairValid reports whether tenant ID and name are both set or
// both empty.
func isTenantPairValid(tenantID, tenantName string) bool {
	return isNonEmpty(tenantID) == isNonEmpty(tenantName)
}

func validateRoamingIdentity(id RoamingIdentity) bool {
	return isNonEmpty(id.PlatformID) &&
		isNonEmpty(id.PlatformName) &&
		isTenantPairValid(id.TenantID, id.TenantName)
}

func validateChargerIdentity(id ChargerIdentity) bool {
	return isNonEmpty(id.ChargerID) &&
		isTenantPairValid(id.TenantID, id.TenantName)
}
