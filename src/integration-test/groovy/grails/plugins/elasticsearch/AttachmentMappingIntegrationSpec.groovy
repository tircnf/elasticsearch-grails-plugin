package grails.plugins.elasticsearch

import grails.testing.mixin.integration.Integration
import grails.gorm.transactions.Rollback
import test.File

@Integration
@Rollback
class AttachmentMappingIntegrationSpec extends EsContainerSpec {

    ElasticSearchService elasticSearchService
    ElasticSearchAdminService elasticSearchAdminService

    void 'Index a File object'() {
        given:
        def contents = "It was the best of times, it was the worst of times"
        def file = new File(filename: 'myTestFile.txt',
                attachment: contents.bytes.encodeBase64().toString())
        file.save(failOnError: true)

        when:
        elasticSearchAdminService.refresh() // Ensure the latest operations have been exposed on the ES instance

        and:
        elasticSearchService.search('best', [indices: File, types: File]).total.value == 1

        then:
        elasticSearchService.unindex(file)
        elasticSearchAdminService.refresh()

        and:
        elasticSearchService.search('best', [indices: File, types: File]).total.value == 0
    }
}
