package grails.plugins.elasticsearch


import grails.converters.JSON
import grails.gorm.transactions.NotTransactional
import grails.gorm.transactions.Rollback
import grails.testing.mixin.integration.Integration
import groovy.util.logging.Slf4j
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.DistanceUnit
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.grails.web.json.JSONObject
import org.hibernate.proxy.HibernateProxy
import spock.lang.IgnoreRest
import spock.lang.Issue
import spock.lang.Unroll
import test.*
import test.custom.id.Toy

import java.math.RoundingMode
import java.time.*

@Integration
@Rollback
@Slf4j
class ElasticSearchServiceIntegrationSpec extends EsContainerSpec implements ElasticSearchSpec {

    ElasticSearchContextHolder elasticSearchContextHolder

    private static final List<Map> EXAMPLE_GEO_BUILDINGS = [
            [lat: 48.13, lon: 11.60, name: '81667'],
            [lat: 48.19, lon: 11.65, name: '85774'],
            [lat: 47.98, lon: 10.18, name: '87700']
    ]

    /**
     * This test class doesn't delete any ElasticSearch indices, because that would also delete the mapping.
     * Be aware of this when indexing new objects, and make sure to remove them in a cleanup block.
     */

    def setup() {
        log.debug "*** SETUP RUNNING"
        ElasticSearchResult searchResult = elasticSearchService.search("")
        if (searchResult.total.value) {
            log.error "A previous test left data in the index"
            searchResult.searchResults.each {
                log.info "${it}  ${it.properties}"
            }
            assert searchResult.total.value == 0, "The previous test left data in the index"
        }
    }


//    @IgnoreRest
    void "startup logging"() {
        expect: "grab all the stdout/logging of the system starting up"
        true
    }


    void "test indexing hibernate proxy"() {
        Person abby = new Person(firstName: 'Abby', lastName: 'Reynolds').save(flush: true)
        Spaceship spaceship = new Spaceship(name: 'Arc', captain: abby).save(flush: true)
        clearSession()

        when:
        spaceship = Spaceship.load(spaceship.id)

        then: "spaceship is proxy"
        spaceship.getClass() in HibernateProxy

        when: "index a proxy instance"
        elasticSearchService.index(spaceship)

        refreshIndex(Spaceship)

        ElasticSearchResult searchResult = search(Spaceship, 'arc')

        then:
        searchResult.total.value == 1

        def result = searchResult.searchResults.first()
        result.name == 'Arc'
        result.captain.firstName == 'Abby'
        result.captain.lastName == 'Reynolds'

        cleanup:
        unindex(spaceship)
        refreshIndex(Spaceship)
    }


    void 'Index and un-index a domain object'() {
        given: "Product that is searchable by default"

        String name = 'myTestProduct'

        when:

        def product = save new Product(productName: name)
        index(product)
        refreshIndex(Product)

        then:
        search(Product, name).total.value == 1

        when: "I unindex the Product"
        unindex(product)
        refreshIndex(Product)

        then:
        search(Product, name).total.value == 0
    }


    void 'Indexing the same object multiple times updates the corresponding ES entry'() {
        given:
        String origProductName = 'myOtherTestProduct'
        def product = save new Product(productName: origProductName)

        when:
        index(product)
        refreshIndex(Product)

        then:
        search(Product, origProductName).total.value == 1

        when:
        String newProductName = "new name"

        product.productName = newProductName
        save product

        index(product)
        refreshIndex(Product)

        then:
        search(Product, origProductName).total.value == 0

        and:
        def result = search(Product, newProductName)
        result.total.value == 1
        List<Product> searchResults = result.searchResults
        searchResults[0].productName == product.productName

        cleanup:
        unindex(product)
        refreshIndex(Product)
    }


    void 'a json object value should be marshalled and de-marshalled correctly'() {
        given:

        String origName = System.currentTimeMillis().toString()
        def product = save new Product(
                productName: origName,
                json: new JSONObject("""{ "test": { "details": "blah" } }"""))

        index(product)
        refreshIndex(Product)

        when:
//        def result = search(Product, product.productName)
        def result = search(Product) {
            bool { must { term("json.test.details": 'wrong') } }
        }

        then:
        result.total.value == 0

        when:
        result = search(Product) {
            bool { must { term("json.test.details": 'blah') } }
        }

        then:
        result.total.value == 1
        List<Product> searchResults = result.searchResults
        searchResults[0].productName == product.productName
        ((Product) searchResults[0]).json.test.details == "blah"

        when: "verify JSONObjects serialize to the DB correctly."
        clearSession()
        Product check = Product.get(product.id)

        then:
        check.json == product.json

        cleanup:
        unindex(product)
        refreshIndex(Product)
    }


    void 'should marshal the alias field and unmarshal correctly (ignore alias)'() {
        given:
        String name = "WatchTower"
        def location = save new GeoPoint(lat: 53.00, lon: 10.00)
        def building = save new Building(name: name, location: location)

        index(building)
        refreshIndex(Building)

        when:
        def result = search(Building, building.name)

        then:
        result.total.value == 1
        List<Building> searchResults = result.searchResults
        searchResults[0].name == building.name

        cleanup:
        unindex(building)
        refreshIndex(Building)
    }

    void 'a date value should be marshalled and de-marshalled correctly'() {
        given:
        String name = "Product with a date value"
        def date = new Date()
        def product = save new Product(productName: name, date: date)

        index(product)
        refreshIndex(Product)

        when:
        def result = search(Product, product.productName)

        then:
        result.total.value == 1
        List<Product> searchResults = result.searchResults
        searchResults[0].productName == product.productName
        searchResults[0].date == product.date

        cleanup:
        unindex(product)
        refreshIndex(Product)
    }

    void 'a temporal type value should be marshalled and de-marshalled correctly'() {
        given:
        String name = "Object with time types"
        def zoneId = ZoneId.ofOffset('', ZoneOffset.ofHours(-4))
        def localDate = LocalDate.now()
        def localDateTime = LocalDateTime.now()
        def zonedDateTime = ZonedDateTime.now(zoneId)
        def offsetDateTime = OffsetDateTime.now(zoneId)
        def offsetTime = OffsetTime.now(zoneId)
        def dates = save new Dates(name: name,
                localDate: localDate,
                localDateTime: localDateTime,
                zonedDateTime: zonedDateTime,
                offsetDateTime: offsetDateTime,
                offsetTime: offsetTime)

        index(dates)
        refreshIndex(Dates)

        when:
        def result = search(Dates, dates.name)

        then:
        result.total.value == 1
        List<Dates> searchResults = result.searchResults
        searchResults[0].name == dates.name
        searchResults[0].localDate == dates.localDate
        //TODO: Workaround with string comparison for JDK 11 as offset info is different
        searchResults[0].localDateTime.toString().substring(0, 19) == dates.localDateTime.toString().substring(0, 19)
        searchResults[0].offsetDateTime.toString().substring(0, 12) == dates.offsetDateTime.toString().substring(0, 12)
        searchResults[0].offsetTime.toString().substring(0, 8) == dates.offsetTime.toString().substring(0, 8)
        searchResults[0].zonedDateTime == dates.zonedDateTime

        cleanup:
        unindex(dates)
        refreshIndex(Dates)
    }

    void 'a geo point location is marshalled and de-marshalled correctly'() {
        given:
        String name = "EvileagueHQ"
        def location = save new GeoPoint(lat: 53.00, lon: 10.00)
        def building = save new Building(name: name, location: location)

        index(building)
        refreshIndex(Building)

        when:
        def result = search(Building, name)

        then:
        result.total.value == 1
        List<Building> searchResults = result.searchResults
        def resultLocation = searchResults[0].location
        resultLocation.lat == location.lat
        resultLocation.lon == location.lon

        cleanup:
        unindex(building)
        refreshIndex(Building)
    }

    void 'a geo point is mapped correctly'() {
        when:
        GeoPoint location = save new GeoPoint(lat: 53.00, lon: 10.00)
        Building building = save new Building(location: location)

        index(building)
        refreshIndex(Building)

        then:
        Map<String, Object> mapping = getFieldMappingMetaData('test.building', 'building').sourceAsMap
        mapping.properties.location.type == 'geo_point'

        cleanup:
        unindex(building)
        refreshIndex(Building)

    }

    void 'search with geo distance filter'() {
        given: 'a building with a geo point location'
        def geoPoint = save new GeoPoint(lat: 50.1, lon: 13.3)
        def building = save new Building(name: 'Test Product', location: geoPoint)

        elasticSearchService.index(building)
        refreshIndex(Building)

        when: 'a geo distance filter search is performed'
        Map params = [indices: Building, types: Building]
        QueryBuilder query = QueryBuilders.matchAllQuery()
        def location = '50, 13'

        Closure filter = {
            'geo_distance'(
                    'distance': '50km',
                    'location': location)
        }

        def result = elasticSearchService.search(params, query, filter)

        then: 'the building should be found'
        1L == result.total.value
        List<Building> searchResults = result.searchResults
        searchResults[0].id == building.id

        cleanup:
        unindex(building)
        refreshIndex(Building)
    }

    void 'searching with filtered query'() {
        given: 'some products'
        loadProducts()

        when: 'searching for a price'
        def result = elasticSearchService.
                search(QueryBuilders.matchAllQuery(), QueryBuilders.rangeQuery("price").gte(1.99).lte(2.3))

        then: "the result should be product 'wurm'"
        result.total.value == 1
        List<Product> searchResults = result.searchResults
        searchResults[0].productName == 'wurm'

        cleanup:
        cleanupProducts()
    }

    List<Product> productList

    private void loadProducts() {
        assert productList == null, "did loadProducts get called twice in the same test?"
        productList = []
        productList << save(new Product(productName: 'horst', price: 3.95))
        productList << save(new Product(productName: 'hobbit', price: 5.99))
        productList << save(new Product(productName: 'best', price: 10.99))
        productList << save(new Product(productName: 'high and supreme', price: 45.50))
        productList << save(new Product(productName: 'wurm', price: 2.00))
        productList << save(new Product(productName: 'hans', price: 0.5))
        productList << save(new Product(productName: 'foo', price: 5.0))

        productList.each {
            index(it)
        }
        refreshIndex(Product)
    }

    private void cleanupProducts() {
        try {
            productList.each {
                unindex(it)
            }
            productList = null
            refreshIndex(Product)
        } catch (Exception e) {
            log.error "Unexpected error during cleanup.  The index might still be modified", e
        }
    }

    void 'searching with a FilterBuilder filter'() {
        when: 'searching for a price'
        loadProducts()

        QueryBuilder filter = QueryBuilders.rangeQuery("price").gte(1.99).lte(2.3)
        def result = elasticSearchService.search(QueryBuilders.matchAllQuery(), filter)

        then: "the result should be product 'wurm'"
        result.total.value == 1
        List<Product> searchResults = result.searchResults
        searchResults[0].productName == "wurm"

        cleanup:
        cleanupProducts()
    }


    void 'searching with wildcards in query at first position'() {
        given:
        loadProducts()

        when: 'search with asterisk at first position'
        def result = search(Product, { wildcard(productName: '*st') })

        then: 'the result should contain 2 products'
        result.total.value == 2
        List<Product> searchResults = result.searchResults
        searchResults*.productName.containsAll('best', 'horst')

        cleanup:
        cleanupProducts()
    }

    void 'searching with wildcards in query at last position'() {
        given:
        loadProducts()

        when: 'search with asterisk at last position'
        Map params2 = [indices: Product, types: Product]
        def result2 = elasticSearchService.search(
                {
                    wildcard(productName: 'ho*')
                }, params2)

        then: 'the result should return 2 products'
        result2.total.value == 2
        List<Product> searchResults2 = result2.searchResults
        searchResults2*.productName.containsAll('horst', 'hobbit')

        cleanup:
        cleanupProducts()
    }

    void 'searching with wildcards in query in between position'() {
        given:
        loadProducts()

        when: 'search with asterisk in between position'
        def result = search(Product) {
            wildcard(productName: 's*eme')
        }

        then: 'the result should return 1 product'
        result.total.value == 1
        List<Product> searchResults3 = result.searchResults
        searchResults3[0].productName == 'high and supreme'

        cleanup:
        cleanupProducts()
    }

    void 'searching for special characters in data pool'() {
        given: 'some products'
        def product = save new Product(productName: 'ästhätik', price: 3.95)

        index(product)
        refreshIndex(Product)

        when: "search for 'a umlaut' "
        def result = elasticSearchService.search({ match(productName: 'ästhätik') })

        then: 'the result should contain 1 product'
        result.total.value == 1
        List<Product> searchResults = result.searchResults
        searchResults[0].productName == product.productName

        cleanup:
        unindex(product)
        refreshIndex(Product)
    }

    void 'Paging and sorting through search results'() {
        given: 'a bunch of products'

        List<Product> productList = []

        10.times {
            def product = save new Product(productName: "Produkt${it}", price: it)
            index(product)
            productList << product
        }
        refreshIndex(Product)

        when: 'a search is performed'
        def params = [from: 3, size: 2, indices: Product, types: Product, sort: 'productName']
        def query = {
            wildcard(productName: 'produkt*')
        }
        def result = elasticSearchService.search(query, params)

        then: 'the correct result-part is returned'
        result.total.value == 10
        result.searchResults.size() == 2
        result.searchResults*.productName == ['Produkt3', 'Produkt4']

        cleanup:
        productList.each {
            unindex it
        }
        refreshIndex(Product)
    }

    void 'Multiple sorting through search results'() {
        given: 'a bunch of products'

        List<Product> productList = []
        def product
        2.times { int i ->
            2.times { int k ->
                product = new Product(productName: "Yogurt$i", price: k).save(failOnError: true, flush: true)
                elasticSearchService.index(product)
                productList << product
            }
        }
        refreshIndex(Product)

        when: 'a search is performed'
        def sort1 = new FieldSortBuilder('productName').order(SortOrder.ASC)
        def sort2 = new FieldSortBuilder('price').order(SortOrder.DESC)
        def params = [indices: Product, types: Product, sort: [sort1, sort2]]
        def query = {
            wildcard(productName: 'yogurt*')
        }
        def result = elasticSearchService.search(query, params)

        then: 'the correct result-part is returned'
        result.searchResults.size() == 4
        result.searchResults*.productName == ['Yogurt0', 'Yogurt0', 'Yogurt1', 'Yogurt1']
        result.searchResults*.price == [1, 0, 1, 0]

        when: 'another search is performed'
        sort1 = new FieldSortBuilder('productName').order(SortOrder.DESC)
        sort2 = new FieldSortBuilder('price').order(SortOrder.ASC)
        params = [indices: Product, types: Product, sort: [sort1, sort2]]
        query = {
            wildcard(productName: 'yogurt*')
        }
        result = elasticSearchService.search(query, params)

        then: 'the correct result-part is returned'
        result.total.value == 4
        result.searchResults.size() == 4
        result.searchResults*.productName == ['Yogurt1', 'Yogurt1', 'Yogurt0', 'Yogurt0']
        result.searchResults*.price == [0, 1, 0, 1]

        cleanup:
        productList.each {
            unindex(it)
        }
        refreshIndex(Product)
    }

    void 'A search with Uppercase Characters should return appropriate results'() {
        given: 'a product with an uppercase name'
        def product = save new Product(productName: 'Großer Kasten', price: 0.85)

        index(product)
        refreshIndex(Product)

        when: 'a search is performed'
        def result = search(Product) {
            match('productName': 'Großer')
        }

        then: 'the correct result-part is returned'
        result.total.value == 1
        result.searchResults.size() == 1
        result.searchResults*.productName == ['Großer Kasten']

        cleanup:
        unindex(product)
        refreshIndex(Product)
    }

    private List<Building> buildingList

    private void setupGeoData() {
        assert buildingList == null
        buildingList = []
        EXAMPLE_GEO_BUILDINGS.each {
            GeoPoint geoPoint = save new GeoPoint(lat: it.lat, lon: it.lon)
            Building b = save new Building(name: "${it.name}", location: geoPoint)
            buildingList << b
            index(b)
        }
        refreshIndex(Building)
    }

    private void cleanupGeoData() {
        buildingList.each {
            unindex(it)
        }
        refreshIndex(Building)
    }

    @Unroll
    void 'a geo distance search finds geo points at varying distances'(String distance, List<String> postalCodesFound) {
        given:
        setupGeoData()

        when: 'a geo distance search is performed'
        Map params = [indices: Building, types: Building]
        QueryBuilder query = QueryBuilders.matchAllQuery()
        def location = [lat: 48.141, lon: 11.57]

        Closure filter = {
            geo_distance(
                    'distance': distance,
                    'location': location)
        }
        def result = elasticSearchService.search(params, query, filter)

        then: 'all geo points in the search radius are found'
        List<Building> searchResults = result.searchResults

        (postalCodesFound.empty && searchResults.empty) ||
                searchResults.each { it.name in postalCodesFound }

        cleanup:
        cleanupGeoData()

        where:
        distance | postalCodesFound
        '1km'    | []
        '5km'    | ['81667']
        '20km'   | ['81667', '85774']
        '1000km' | ['81667', '85774', '87700']
    }

    void 'A search with lowercase Characters should return appropriate results'() {
        given: 'a product with a lowercase name'
        def product = save new Product(productName: 'KLeiner kasten', price: 0.45)

        index(product)
        refreshIndex(Product)

        when: 'a search is performed'
        def result = search(Product) {
            wildcard('productName': 'klein*')
        }

        then: 'the correct result-part is returned'
        result.total.value == 1
        result.searchResults.size() == 1
        result.searchResults*.productName == ['KLeiner kasten']

        cleanup:
        unindex(product)
        refreshIndex(Product)
    }

    void 'the distances are returned'() {
        given:
        setupGeoData()

        when: 'a geo distance search is sorted by distance'

        def sortBuilder = SortBuilders.geoDistanceSort('location', 48.141d, 11.57d).
                unit(DistanceUnit.KILOMETERS).
                order(SortOrder.ASC)

        Map params = [indices: Building, types: Building, sort: sortBuilder]
        QueryBuilder query = QueryBuilders.matchAllQuery()
        def location = [lat: 48.141, lon: 11.57]

        Closure filter = {
            geo_distance(
                    'distance': '5km',
                    'location': location)
        }
        def result = elasticSearchService.search(params, query, filter)

        and:
        List<Building> searchResults = result.searchResults
        //Avoid double precission issues
        def sortResults = result.sort.(searchResults[0].id).
                collect { (it as BigDecimal).setScale(4, RoundingMode.HALF_UP) }

        then: 'all geo points in the search radius are found'
        sortResults == [2.5401]

        cleanup:
        cleanupGeoData()
    }

    void 'Component as an inner object'() {
        given:
        def mal = save new Person(firstName: 'Malcolm', lastName: 'Reynolds')
        def spaceship = save new Spaceship(name: 'Serenity', captain: mal)

        index(spaceship)
        refreshIndex(Spaceship)

        when:
        def searchResult = search(Spaceship, 'serenity')

        then:
        searchResult.total.value == 1

        def result = searchResult.searchResults.first()
        result.name == 'Serenity'
        result.captain.firstName == 'Malcolm'
        result.captain.lastName == 'Reynolds'

        cleanup:
        unindex(spaceship)
        refreshIndex(Spaceship)
    }

    //
    void 'Multi_filed creates untouched field'() {
        given:
        def mal = save new Person(firstName: 'J. T.', lastName: 'Esteban')
        def spaceship = save new Spaceship(name: 'USS Grissom', captain: mal)

        index(spaceship)
        refreshIndex(Spaceship)

        when:
        def searchResult = search(Spaceship) {
            bool { must { term("name.untouched": 'USS Grissom') } }
        }

        then:
        searchResult.total.value == 1

        def result = searchResult.searchResults.first()
        result.name == 'USS Grissom'
        result.captain.firstName == 'J. T.'
        result.captain.lastName == 'Esteban'

        cleanup:
        unindex(spaceship)
        refreshIndex(Spaceship)
    }

    //
    void 'Fields creates creates child field'() {
        given:
        def mal = save new Person(firstName: 'Jason', lastName: 'Lambert')
        def spaceship = save new Spaceship(name: 'Intrepid', captain: mal)

        index(spaceship)
        refreshIndex(Spaceship)

        when:
        def searchResult = search(Spaceship) {
            bool { must { term("captain.firstName.raw": 'Jason') } }
        }

        then:
        searchResult.total.value == 1

        def result = searchResult.searchResults.first()
        result.name == 'Intrepid'
        result.captain.firstName == 'Jason'
        result.captain.lastName == 'Lambert'

        cleanup:
        unindex(spaceship)
        refreshIndex(Spaceship)
    }

    //
    void 'dynamicly mapped JSON strings should be searchable'() {
        given: 'A Spaceship with some cool cannons'
        def spaceship = new Spaceship(
                name: 'Spaceball One', captain: new Person(firstName: 'Dark', lastName: 'Helmet').save())
        def data = [engines   : [[name: "Primary", maxSpeed: 'Ludicrous Speed'],
                                 [name: "Secondary", maxSpeed: 'Ridiculous Speed'],
                                 [name: "Tertiary", maxSpeed: 'Light Speed'],
                                 [name: "Main", maxSpeed: 'Sub-Light Speed'],],
                    facilities: ['Shopping Mall', 'Zoo', 'Three-Ring circus']]
        spaceship.shipData = (data as JSON).toString()
        spaceship.save(flush: true, validate: false)

        index(spaceship)
        refreshIndex(Spaceship)

        when: 'a search is executed'
        def searchResult = search(Spaceship) {
            bool { must { term("shipData.engines.name": 'primary') } }
        }

        then: "the json data should be searchable as if it was an actual component of the Spaceship"
        searchResult.total.value == 1
        def result = searchResult.searchResults.first()
        def shipData = JSON.parse(result.shipData)

        result.name == 'Spaceball One'
        shipData.facilities.size() == 3


        cleanup:
        unindex(spaceship)
        refreshIndex(Spaceship)
    }

    void 'Index a domain object with UUID-based id and custom identity name'() {
        given:
        def car = save new Toy(name: 'Car', color: "Red")
        def plane = save new Toy(name: 'Plane', color: "Yellow")

        index(car, plane)
        refreshIndex(Toy)

        when:
        def searchResult = search(Toy, 'Yellow')

        then:
        searchResult.total.value == 1
        searchResult.searchResults[0].id == plane.id

        cleanup:
        unindex(car, plane)
        refreshIndex(Toy)
    }

    void 'Bulk Index a domain object with UUID-based id and custom identity name'() {
        given:
        Long oldValue = elasticSearchContextHolder.config.maxBulkRequest
        elasticSearchContextHolder.config.maxBulkRequest = 5
        100.times {def car = save new Toy(name: 'Car', color: "Red")}
        def plane = save new Toy(name: 'Plane', color: "Yellow")

        index(Toy)
        refreshIndex(Toy)

        when:
        def searchResult = search(Toy, 'Yellow')

        then:
        searchResult.total.value == 1
        searchResult.searchResults[0].toyId == plane.toyId

        cleanup:
        println "Cleanup running"
        elasticSearchContextHolder.config.maxBulkRequest = oldValue
        unindex(Toy)
        refreshIndex(Toy)
    }

    private void createBulkData() {
        1858.times { n ->
            def person = save(new Person(firstName: 'Person', lastName: "McNumbery$n"), false)
            save(new Spaceship(name: "Ship-$n", captain: person), false)
        }
        flushSession()
    }

    void 'bulk test'() {
        given:
        createBulkData()

        when:
        index(Spaceship)

        refreshIndex(Spaceship)

        then:
        findFailures().size() == 0
        elasticSearchService.countHits('Ship\\-') == 1858

        cleanup:
        unindex(Spaceship)
        refreshIndex(Spaceship)
    }

    void 'Use an aggregation'() {
        given:
        def jim = save new Product(productName: 'jim', price: 1.99)
        def xlJim = save new Product(productName: 'xl-jim', price: 5.99)

        index(jim, xlJim)
        refreshIndex(Product)

        when:
        def searchResult = elasticSearchService.search(
                QueryBuilders.matchQuery('productName', 'jim'),
                null as Closure,
                AggregationBuilders.max('max_price').field('price'))

        then:
        searchResult.total.value == 2
        searchResult.aggregations.'max_price'.value == 5.99f

        cleanup:
        unindex(jim, xlJim)
        refreshIndex(Product)
    }

    @NotTransactional
    @Issue("https://github.com/puneetbehl/elasticsearch-grails-plugin/issues/30")
    def "parent is still found when child is removed"() {
        given: "a parent with a component child"
        Parent parent
        Parent.withNewTransaction {
            parent = new Parent(name: 'foo')
            parent.addToChildren(new Child())
            parent.save(failOnError: true)
        }
        elasticSearchAdminService.refresh()

        expect: "parent is found"
        elasticSearchService.search('foo', [indices: Parent, types: Parent]).total.value == 1

        when: "child is removed from parent"
        Parent.withNewTransaction {
            parent.children*.delete()
            parent.children.clear()
            parent.save(failOnError: true)
        }
        elasticSearchAdminService.refresh()

        then: "parent is still found"
        elasticSearchService.search('foo', [indices: Parent, types: Parent]).total.value == 1

        cleanup:
        Parent.withNewTransaction {
            parent.delete(flush: true)
            unindex(parent)
        }
        refreshIndex(Parent)
    }


    //
    def "Verify Bulk Index works when count < maxId"() {
        given: "some posts"
        def oldBulkSize = elasticSearchContextHolder.config.maxBulkRequest
        elasticSearchContextHolder.config.maxBulkRequest = 5
        List<Product> products = []
        (1..50).each {
            products << save(new Product(productName: "product $it"))
        }

        and: "I delete some"
        products[10..39].each {
            it.delete(flush: true)
        }

        long productCount = Product.count()

        when: ""

        index(Product)
        refreshIndex(Product)

        then:
        search("").getTotal().value == productCount

        cleanup:
        unindex(Product)
        refreshIndex(Product)
        elasticSearchContextHolder.config.maxBulkRequest = oldBulkSize

    }

    private def findFailures() {
        def domainClass = getDomainClass(Spaceship)
        def failures = []
        def allObjects = Spaceship.list()
        allObjects.each {
            elasticSearchHelper.withElasticSearch { client ->
                GetRequest getRequest = new GetRequest(
                        getIndexName(domainClass), it.id.toString())
                GetResponse result = client.get(getRequest, RequestOptions.DEFAULT)
                if (!result.isExists()) {
                    failures << it
                }
            }
        }
        failures
    }

}
