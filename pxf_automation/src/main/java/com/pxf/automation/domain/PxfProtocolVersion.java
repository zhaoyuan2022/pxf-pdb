package com.pxf.automation.domain;

/**
 * POJO Class that is used to represent entity, exposed by
 * org.apache.hawq.pxf.service.rest.VersionResource
 */
public class PxfProtocolVersion {
	public String version;

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}
}