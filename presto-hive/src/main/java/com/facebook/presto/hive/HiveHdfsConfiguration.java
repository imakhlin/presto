/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION;

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");

        // must not be transitively reloaded during the future loading of various Hadoop modules
        // all the required default resources must be declared above
        INITIAL_CONFIGURATION = new Configuration(false);
        Configuration defaultConfiguration = new Configuration();
        copy(defaultConfiguration, INITIAL_CONFIGURATION);
    }

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            updater.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationUpdater updater;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationUpdater updater)
    {
        this.updater = requireNonNull(updater, "updater is null");
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        // use the same configuration for everything
        Configuration conf = hadoopConfiguration.get();
        if (context.getIdentity().getPrincipal().isPresent()) {
            String name = context.getIdentity().getPrincipal().get().getName();
            //there's a presto bug that workers get toString here instead of the actual getName value
            if (name.contains("V3IOPrincipal")) {
                String[] tokenparse = name.split("token=");
                if (tokenparse.length != 2) {
                    throw new RuntimeException("could not parse token from v3io principal, name=" + name);
                }
                else {
                    conf.set("v3io.client.session.access-key", tokenparse[1].substring(0, tokenparse[1].length() - 1));
                }
            }
            else {
                conf.set("v3io.client.session.access-key", name);
            }
        }
        return conf;
    }
}
