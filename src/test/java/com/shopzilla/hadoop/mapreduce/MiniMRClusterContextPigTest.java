/**
 * Copyright 2012 Shopzilla.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  http://tech.shopzilla.com
 *
 */

package com.shopzilla.hadoop.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Jeremy Lucas
 * @since 6/8/12
 */
@RunWith (SpringJUnit4ClassRunner.class)
@ContextConfiguration ("classpath:mini-mr-cluster-context.xml")
public class MiniMRClusterContextPigTest {

    @Resource (name = "miniMrClusterContext")
    private MiniMRClusterContext miniMRClusterContext;

    @Resource (name = "hadoopConfig")
    private Configuration configuration;

    @Test
    public void testPig() throws Exception {
        final AtomicInteger inputRecordCount = new AtomicInteger(0);
        miniMRClusterContext.processData(new Path("/user/test/keywords_data"), new Function<String, Void>() {
            @Override
            public Void apply(String line) {
                inputRecordCount.incrementAndGet();
                return null;
            }
        });

        Map<String, String> params = ImmutableMap.<String, String>builder()
            .put("INPUT", "/user/test/keywords_data")
            .put("OUTPUT", "/user/test/keywords_output")
            .build();
        PigServer pigServer = miniMRClusterContext.getPigServer();
        pigServer.registerScript(new ClassPathResource("some_pig_script.pig").getInputStream(), params);
        pigServer.setBatchOn();
        pigServer.executeBatch();

        final AtomicInteger outputRecordCount = new AtomicInteger(0);

        miniMRClusterContext.processData(new Path("/user/test/keywords_output"), new Function<String, Void>() {
            @Override
            public Void apply(String line) {
                outputRecordCount.incrementAndGet();
                return null;
            }
        });

        assertEquals(inputRecordCount.intValue(), outputRecordCount.intValue());
    }
}
