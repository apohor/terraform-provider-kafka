package sarama

type DescribeAclsRequest struct {
	Version int
	AclFilter
}

func (d *DescribeAclsRequest) encode(pe packetEncoder) error {
	d.AclFilter.Version = d.Version
	return d.AclFilter.encode(pe)
}

func (d *DescribeAclsRequest) decode(pd packetDecoder, version int16) (err error) {
	return d.AclFilter.decode(pd, version)
}

func (d *DescribeAclsRequest) key() int16 {
	return 29
}

func (d *DescribeAclsRequest) version() int16 {
	return int16(d.Version)
}

func (d *DescribeAclsRequest) requiredVersion() KafkaVersion {
	switch d.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}
