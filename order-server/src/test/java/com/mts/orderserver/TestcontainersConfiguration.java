package com.mts.orderserver;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	MySQLContainer mysqlContainer() {
		return new MySQLContainer(DockerImageName.parse("mysql:8.4.8"))
			.withDatabaseName("trading")
			.withCopyFileToContainer(
					MountableFile.forHostPath("../infra/mysql/init/01-schema.sql"),
					"/docker-entrypoint-initdb.d/01-schema.sql");
	}

}
