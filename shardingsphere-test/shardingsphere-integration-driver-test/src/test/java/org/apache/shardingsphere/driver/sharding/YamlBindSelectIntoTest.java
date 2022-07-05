/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.driver.sharding;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Objects;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.driver.AbstractYamlDataSourceTest;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.junit.Test;

@RequiredArgsConstructor
public class YamlBindSelectIntoTest extends AbstractYamlDataSourceTest {
    
    @Test
    public void assertWithDataSource() throws Exception {
        String filePath = "/yaml/integrate/sharding/configBindAndBroadcastDatasource.yaml";
        File yamlFile = new File(Objects.requireNonNull(YamlShardingIntegrateTest.class.getResource(filePath)).toURI());
        DataSource dataSource = YamlShardingSphereDataSourceFactory.createDataSource(yamlFile);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("insert into t_user_bak select * from t_user");
            statement.execute("insert into t_config_bak select * from t_config");
        }
        ((ShardingSphereDataSource) dataSource).close();
    }
}
