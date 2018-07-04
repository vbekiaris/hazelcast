#!/usr/bin/env bash
# -Dhazelcast.config=./hazelcast.xml
native-image \
 -Dhazelcast.phone.home.enabled=false \
 -Djava.util.logging.config.file=logging.properties \
 -R:MaximumHeapSizePercent=40 \
 -H:+ReportUnsupportedElementsAtRuntime \
 -H:IncludeResources="(\S*.xml|\S*.xsd|\S*.properties)" \
 -H:ReflectionConfigurationFiles=src/main/nativeimage/reflection_config.json \
 -H:+JNI \
 -H:JNIConfigurationFiles=src/main/nativeimage/jni_config.json \
 -H:EnableURLProtocols=resource,http \
 -H:IncludeResourceBundles=com.sun.org.apache.xml.internal.serializer.utils.SerializerMessages \
 --verbose \
 -jar target/hazelcast-graalvm-3.11-SNAPSHOT-jar-with-dependencies.jar


# -H:IncludeResources="(META-INF\/services\/.*|.*\.xsd|.*\.xml|\S*.properties|org\/apache\/.*\/\S*.properties)" \
