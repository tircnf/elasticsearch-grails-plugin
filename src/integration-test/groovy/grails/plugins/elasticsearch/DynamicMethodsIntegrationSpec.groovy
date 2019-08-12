package grails.plugins.elasticsearch

import grails.gorm.transactions.Rollback
import grails.testing.mixin.integration.Integration
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import spock.lang.Specification
import test.Photo


@Integration
@Rollback
class DynamicMethodsIntegrationSpec extends Specification implements ElasticSearchSpec {

    def setup() {
        save new Photo(name: 'Captain Kirk', url: 'http://www.nicenicejpg.com/100')
        save new Photo(name: 'Captain Picard', url: 'http://www.nicenicejpg.com/200')
        save new Photo(name: 'Captain Sisko', url: 'http://www.nicenicejpg.com/300')
        save new Photo(name: 'Captain Janeway', url: 'http://www.nicenicejpg.com/400')
        save new Photo(name: 'Captain Archer', url: 'http://www.nicenicejpg.com/500')
    }

    def cleanup() {
        Photo.list().each { it.delete() }
    }

    void 'can search using Dynamic Methods'() {
        given:
            refreshIndices()

        expect:
            search(Photo, 'Captain').total.value == 5

        when:
            def results = Photo.search {
                match(name: 'Captain')
            }

        then:
            results.total.value == 5
            results.searchResults.every { it.name =~ /Captain/ }
    }

    void 'can search and filter using Dynamic Methods'() {
        given:
            refreshIndices()

        expect:
            search(Photo, 'Captain').total.value == 5

        when:
            def results = Photo.search({
                match(name: 'Captain')
            }, {
                match {
                    "url"(query: "http://www.nicenicejpg.com/100", operator: "and")
                }
            })

        then:
            results.total.value == 1
            results.searchResults[0].name == 'Captain Kirk'
    }

    void 'can search using a QueryBuilder and Dynamic Methods'() {
        given:
            refreshIndices()

        expect:
            search(Photo, 'Captain').total.value == 5

        when:
            QueryBuilder query = QueryBuilders.matchQuery('url', 'http://www.nicenicejpg.com/100').operator(Operator.AND)
            def results = Photo.search(query)

        then:
            results.total.value == 1
            results.searchResults[0].name == 'Captain Kirk'
    }

    void 'can search using a QueryBuilder, a filter and Dynamic Methods'() {
        given:
            refreshIndices()

        expect:
            search(Photo, 'Captain').total.value == 5

        when:
            QueryBuilder query = QueryBuilders.matchQuery('name', 'Captain')
            def results = Photo.search(query,
                    {
                        match {
                            "url"(query: "http://www.nicenicejpg.com/100", operator: "and")
                        }
                    })

        then:
            results.total.value == 1
            results.searchResults[0].name == 'Captain Kirk'
    }

    void 'can search using a QueryBuilder, a FilterBuilder and Dynamic Methods'() {
        given:
            refreshIndices()

        expect:
            search(Photo, 'Captain').total.value == 5

        when:
            QueryBuilder query = QueryBuilders.matchAllQuery()
            QueryBuilder filter = QueryBuilders.matchQuery('url', 'http://www.nicenicejpg.com/100').operator(Operator.AND)
            def results = Photo.search(query, filter)

        then:
            results.total.value == 1
            results.searchResults[0].name == 'Captain Kirk'
    }

    void 'can search and filter using Dynamic Methods and a QueryBuilder'() {
        given:
            refreshIndices()

        expect:
            search(Photo, 'Captain').total.value == 5

        when:
            QueryBuilder filter = QueryBuilders.matchQuery('url', 'http://www.nicenicejpg.com/100').operator(Operator.AND)
            def results = Photo.search({
                match(name: 'Captain')
            }, filter)

        then:
            results.total.value == 1
            results.searchResults[0].name == 'Captain Kirk'
    }
}
