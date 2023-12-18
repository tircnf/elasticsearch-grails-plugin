import grails.util.BuildSettings
import grails.util.Environment

// See http://logback.qos.ch/manual/groovy.html for details on configuration
appender('STDOUT', ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%level %logger - %msg%n"
    }
}

root(ERROR, ['STDOUT'])

def targetDir = BuildSettings.TARGET_DIR
if (Environment.isDevelopmentMode() && targetDir) {
    appender("FULL_STACKTRACE", FileAppender) {
        file = "${targetDir}/stacktrace.log"
        append = true
        encoder(PatternLayoutEncoder) {
            pattern = "%level %logger - %msg%n"
        }
    }
    logger("StackTrace", ERROR, ['FULL_STACKTRACE'], false)
}

//logger("org.apache.http.wire", DEBUG, ['STDOUT'], false)
//logger("grails.plugins.elasticsearch", DEBUG, ['STDOUT'], false)
//logger("org.codehaus.groovy.grails", ERROR, ['STDOUT'], false)
//logger("org.springframework", ERROR, ['STDOUT'], false)
//logger("org.hibernate", ERROR, ['STDOUT'], false)
//logger("net.sf.ehcache.hibernate", ERROR, ['STDOUT'], false)

