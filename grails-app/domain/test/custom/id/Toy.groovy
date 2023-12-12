package test.custom.id

class Toy {
    UUID toyId
    String name
    String color

    static searchable = true

    static mapping = {
        id( name: 'toyId', generator: "uuid2", type: "uuid-char", length: 36)
    }

    static constraints = {
        name(nullable: true)
    }
}
