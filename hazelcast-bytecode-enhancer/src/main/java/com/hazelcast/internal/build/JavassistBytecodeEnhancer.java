package com.hazelcast.internal.build;

import de.icongmbh.oss.maven.plugin.javassist.ClassTransformer;
import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

/**
 * Enhance {@link com.hazelcast.core.IMap} and its implementations at build time with additional
 * methods to avoid breaking binary compatibility.
 * This class is only used during build time, so it would make sense to remove it to a separate module.
 */
public class JavassistBytecodeEnhancer extends ClassTransformer {

    static final String CLASSNAME_IMAP = "com.hazelcast.core.IMap";
    static final String CLASSNAME_ICOMPLETABLE_FUTURE = "com.hazelcast.core.ICompletableFuture";
    static final String CLASSNAME_FUTURE = "java.util.concurrent.Future";
    static final String CLASSNAME_OBJECT = "java.lang.Object";

    @Override
    protected boolean shouldTransform(CtClass candidateClass)
            throws Exception {
        if (candidateClass.isInterface() && candidateClass.getName().equals(CLASSNAME_IMAP)) {
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    protected void applyTransformations(CtClass ctClass)
            throws Exception {
        String className = ctClass.getName();
        if (ctClass.isInterface() && className.equals(CLASSNAME_IMAP)) {
            enhanceIMap(ctClass);
        }
    }

    private void enhanceIMap(CtClass imap) {
        try {
            CtClass future = imap.getClassPool().get(CLASSNAME_FUTURE);
            CtClass object = imap.getClassPool().get(CLASSNAME_OBJECT);
            CtMethod getAsync = CtNewMethod.abstractMethod(future, "getAsync", new CtClass[] {object}, null, imap);
            imap.addMethod(getAsync);
        } catch (NotFoundException e) {
            System.err.println("Failed to enhance IMap: " + e.getMessage());
            e.printStackTrace(System.err);
        } catch (CannotCompileException e) {
            System.err.println("Failed to enhance IMap: " + e.getMessage());
            e.printStackTrace(System.err);
        }

    }
}
