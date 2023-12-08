package grails.plugins.elasticsearch.mapping

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.ElasticSearchContextHolder
import grails.plugins.elasticsearch.EsContainerSpec
import grails.testing.mixin.integration.Integration
import spock.lang.Unroll
import test.Building
import test.Person
import test.Product
import test.mapping.migration.Catalog
import test.transients.Anagram
import test.transients.Palette

/**
 * Created by @marcos-carceles on 28/01/15.
 */
@Integration
class ElasticSearchMappingFactorySpec extends EsContainerSpec {

    GrailsApplication grailsApplication
    SearchableClassMappingConfigurator searchableClassMappingConfigurator
    ElasticSearchContextHolder elasticSearchContextHolder

    void setup() {
        grailsApplication.config.elasticSearch.includeTransients = true
        searchableClassMappingConfigurator.configureAndInstallMappings()
    }

    void cleanup() {
        grailsApplication.config.elasticSearch.includeTransients = false
        searchableClassMappingConfigurator.configureAndInstallMappings()
    }


    @Unroll('#clazz / #property is mapped as #expectedType')
    void "calculates the correct ElasticSearch types"() {
        given:
        SearchableClassMapping scm = elasticSearchContextHolder.getMappingContextByType(clazz)

        when:
        Map mapping = ElasticSearchMappingFactory.getElasticMapping(scm)

        then:
        mapping[clazz.simpleName.toLowerCase()]['properties'][property].type == expectedType

        where:
        clazz    | property          || expectedType

        Building | 'name'            || 'text'
        Building | 'date'            || 'date'
        Building | 'localDate'       || 'date'
        Building | 'location'        || 'geo_point'

        Product  | 'price'           || 'float'
        Product  | 'json'            || 'object'

        Catalog  | 'pages'           || 'object'

        Person   | 'fullName'        || 'text'
        Person   | 'nickNames'       || 'text'

        Palette  | 'colors'          || 'text'
        Palette  | 'complementaries' || 'text'

        Anagram  | 'length'          || 'integer'
        Anagram  | 'palindrome'      || 'boolean'
    }
}
