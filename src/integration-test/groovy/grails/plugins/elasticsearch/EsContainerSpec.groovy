package grails.plugins.elasticsearch

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import groovy.util.logging.Slf4j
import org.testcontainers.elasticsearch.ElasticsearchContainer
import spock.lang.Specification

@Slf4j
class EsContainerSpec extends Specification {

    static ElasticsearchContainer container

    def setupSpec() {

        try {

            def port = (System.getenv("ELASTICSEARCH_PORT") ?: 9200) as Integer
            if (!container) {
                log.info("Starting elastic search on port $port  ")
                container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.17.15")
                        .withExposedPorts(9200)
                        .withCreateContainerCmdModifier({ cmd ->
                            cmd.withHostConfig(
                                    new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(9200)))
                            )
                        });
                container.start()
            } else {
                log.info("ElasticsearchContainer already running on port $port")
            }
        } catch (Exception e) {
            log.error("Unable to start the container", e)
        }
    }

}
