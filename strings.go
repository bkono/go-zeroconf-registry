package zeroconf

import "strings"

// CleanServiceName converts a normal micro service name to a zeroconf compliant format of _service_with_dots_replace._tcp
func CleanServiceName(serviceName string) string {
	zn := strings.TrimSuffix(serviceName, ".")
	zn = strings.Replace(zn, ".", "_", -1) + "._tcp"
	if !strings.HasPrefix(zn, "_") {
		zn = "_" + zn
	}

	return zn
}
