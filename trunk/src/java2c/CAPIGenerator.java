/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package java2c;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fuse.FuseContext;
import fuse.FuseFS;
import fuse.FuseFSDirEnt;
import fuse.FuseFSDirFiller;
import fuse.FuseGetattr;
import fuse.FuseOpen;
import fuse.FuseSize;
import fuse.FuseStatfs;

public class CAPIGenerator {
	private Class<?> clazz;

	public CAPIGenerator(Class<?> clazz) {
		this.clazz = clazz;
	}

	public void generateClassAPI(PrintWriter hOut, PrintWriter cOut,
			boolean extraArgForInterfaces, boolean includeStaticFields) {
		String className = clazz.getName();
		String jniClassName = className.replace('.', '/');
		String structTypeName = "jclass_" + className.replace('.', '_');
		String structVarName = className.replace('.', '_');

		// split all public fields into static and instance fields
		Field[] fields = clazz.getFields();
		List<Field> staticFieldsList = new ArrayList<Field>();
		List<Field> instanceFieldsList = new ArrayList<Field>();
		for (int i = 0; i < fields.length; i++) {
			Field field = fields[i];
			int mod = field.getModifiers();
			if (Modifier.isPublic(mod)) {
				if (Modifier.isStatic(mod))
					staticFieldsList.add(field);
				else
					instanceFieldsList.add(field);
			}
		}
		Field[] staticFields = staticFieldsList
				.toArray(new Field[staticFieldsList.size()]);
		Field[] instanceFields = instanceFieldsList
				.toArray(new Field[instanceFieldsList.size()]);

		// obtain all public constructors and give them C names
		Constructor<?>[] constructors = clazz.getConstructors();
		List<Constructor<?>> constructorsList = new ArrayList<Constructor<?>>();
		Map<Constructor<?>, String> constructor2name = new HashMap<Constructor<?>, String>();
		for (int i = 0; i < constructors.length; i++) {
			Constructor<?> constructor = constructors[i];
			if (Modifier.isPublic(constructor.getModifiers())) {
				constructorsList.add(constructor);
				constructor2name.put(
						constructor,
						getMethodName(null, "new",
								constructor.getParameterTypes()));
			}
		}
		constructors = constructorsList
				.toArray(new Constructor[constructorsList.size()]);

		// split all public methods into static and instance methods and give
		// them C names
		Method[] methods = clazz.getMethods();
		List<Method> staticMethodsList = new ArrayList<Method>();
		List<Method> instanceMethodsList = new ArrayList<Method>();
		Map<Method, String> method2name = new HashMap<Method, String>();
		for (int i = 0; i < methods.length; i++) {
			Method method = methods[i];
			int mod = method.getModifiers();
			if (Modifier.isPublic(mod)) {
				if (Modifier.isStatic(mod))
					staticMethodsList.add(method);
				else
					instanceMethodsList.add(method);

				method2name.put(
						method,
						getMethodName(method.getReturnType(), method.getName(),
								method.getParameterTypes()));
			}
		}
		Method[] staticMethods = staticMethodsList
				.toArray(new Method[staticMethodsList.size()]);
		Method[] instanceMethods = instanceMethodsList
				.toArray(new Method[instanceMethodsList.size()]);

		// before we begin, we output header

		hOut.print("\n" + "/**\n" + " * structure with a reference to "
				+ className + " java class and cached field & method IDs\n"
				+ " */\n" + "typedef struct _" + structTypeName + "\n" + "{\n"
				+ "   // a pointer to globaly referenced Java class\n"
				+ "   jclass class;\n" + "\n");

		// 1st output static fields

		if (staticFields.length > 0 && includeStaticFields) {
			hOut.print("   // cached static field IDs\n" + "   struct\n"
					+ "   {\n");

			for (int i = 0; i < staticFields.length; i++) {
				Field field = staticFields[i];
				hOut.print("      jfieldID " + field.getName() + ";\n");
			}

			hOut.print("\n" + "   } static_field;\n" + "\n");
		}

		// 2nd output instance fields

		if (instanceFields.length > 0) {
			hOut.print("   // cached instance field IDs\n" + "   struct\n"
					+ "   {\n");

			for (int i = 0; i < instanceFields.length; i++) {
				Field field = instanceFields[i];
				hOut.print("      jfieldID " + field.getName() + ";\n");
			}

			hOut.print("\n" + "   } field;\n" + "\n");
		}

		// 3rd output constructors

		if (constructors.length > 0) {
			hOut.print("   // cached constructor IDs\n" + "   struct\n"
					+ "   {\n");

			for (int i = 0; i < constructors.length; i++) {
				Constructor<?> constructor = constructors[i];
				hOut.print("      jmethodID "
						+ constructor2name.get(constructor) + ";\n");
			}

			hOut.print("\n" + "   } constructor;\n" + "\n");
		}

		// 4th output static methods

		if (staticMethods.length > 0) {
			hOut.print("   // cached static method IDs\n" + "   struct\n"
					+ "   {\n");

			for (int i = 0; i < staticMethods.length; i++) {
				Method method = staticMethods[i];
				hOut.print("      jmethodID " + method2name.get(method) + ";\n");
			}

			hOut.print("\n" + "   } static_method;\n" + "\n");
		}

		// 5th output instance methods

		if (instanceMethods.length > 0) {
			hOut.print("   // cached instance method IDs\n" + "   struct\n"
					+ "   {\n");

			for (int i = 0; i < instanceMethods.length; i++) {
				Method method = instanceMethods[i];
				hOut.print("      jmethodID " + method2name.get(method) + ";\n");
			}

			hOut.print("\n" + "   } method;\n" + "\n");
		}

		// and finaly the closing brace for structure...

		hOut.print("} " + structTypeName + ";\n" + "\n");

		// now output to .c & .h file
		// free_* method

		hOut.print("// free structure\n" + "void free_" + structTypeName
				+ "(JNIEnv *env, " + structTypeName + " *" + structVarName
				+ ");\n" + "\n");

		cOut.print("/**\n" + " * free structure with a reference to "
				+ className + " java class and cached field & method IDs\n"
				+ " */\n" + "void free_" + structTypeName + "(JNIEnv *env, "
				+ structTypeName + " *" + structVarName + ")\n" + "{\n"
				+ "   if (" + structVarName + "->class != NULL)\n"
				+ "      (*env)->DeleteGlobalRef(env, " + structVarName
				+ "->class);\n" + "\n" + "   free(" + structVarName + ");\n"
				+ "}\n" + "\n");

		// alloc_* method

		boolean extraArg = extraArgForInterfaces
				&& Modifier.isInterface(clazz.getModifiers());
		String cnArg = extraArg ? ", const char *className" : "";
		String cnVar = extraArg ? "className" : "\"" + jniClassName + "\"";

		hOut.print("// alloc structure\n" + structTypeName + " *alloc_"
				+ structTypeName + "(JNIEnv *env" + cnArg + ");\n" + "\n");

		cOut.print("/**\n" + " * alloc structure with a reference to "
				+ className + " java class and cached field & method IDs\n"
				+ " */\n" + structTypeName + " *alloc_" + structTypeName
				+ "(JNIEnv *env" + cnArg + ")\n" + "{\n" + "   jclass class;\n"
				+ "\n" + "   " + structTypeName + " *" + structVarName + " = ("
				+ structTypeName + "*)calloc(1, sizeof(" + structTypeName
				+ "));\n" + "   if (" + structVarName + " == NULL)\n"
				+ "   {\n" + "      WARN(\"Can't allocate structure "
				+ structTypeName + "\");\n" + "      return NULL;\n" + "   }\n"
				+ "\n" + "   while (1)\n" + "   {\n"
				+ "      class = (*env)->FindClass(env, " + cnVar + ");\n"
				+ "      if ((*env)->ExceptionCheck(env)) break;\n" + "\n"
				+ "      " + structVarName
				+ "->class = (*env)->NewGlobalRef(env, class);\n"
				+ "      if ((*env)->ExceptionCheck(env)) break;\n" + "\n");

		// static fields

		if (staticFields.length > 0 && includeStaticFields) {
			cOut.print("      // obtain static field IDs\n");

			for (int i = 0; i < staticFields.length; i++) {
				Field field = staticFields[i];
				cOut.print("      " + structVarName + "->static_field."
						+ field.getName() + " = (*env)->GetStaticFieldID(env, "
						+ structVarName + "->class, \"" + field.getName()
						+ "\", \"" + getJVMTypeSignature(field.getType())
						+ "\");\n"
						+ "      if ((*env)->ExceptionCheck(env)) break;\n");
			}

			cOut.print("\n");
		}

		// instance fields

		if (instanceFields.length > 0) {
			cOut.print("      // obtain instance field IDs\n");

			for (int i = 0; i < instanceFields.length; i++) {
				Field field = instanceFields[i];
				cOut.print("      " + structVarName + "->field."
						+ field.getName() + " = (*env)->GetFieldID(env, "
						+ structVarName + "->class, \"" + field.getName()
						+ "\", \"" + getJVMTypeSignature(field.getType())
						+ "\");\n"
						+ "      if ((*env)->ExceptionCheck(env)) break;\n");
			}

			cOut.print("\n");
		}

		// constructors

		if (constructors.length > 0) {
			cOut.print("      // obtain constructor method IDs\n");

			for (int i = 0; i < constructors.length; i++) {
				Constructor<?> constructor = constructors[i];
				cOut.print("      " + structVarName + "->constructor."
						+ constructor2name.get(constructor)
						+ " = (*env)->GetMethodID(env, " + structVarName
						+ "->class, \"" + "<init>" + "\", \""
						+ getJVMConstructorSignature(constructor) + "\");\n"
						+ "      if ((*env)->ExceptionCheck(env)) break;\n");
			}

			cOut.print("\n");
		}

		// static methods

		if (staticMethods.length > 0) {
			cOut.print("      // obtain static method IDs\n");

			for (int i = 0; i < staticMethods.length; i++) {
				Method method = staticMethods[i];
				cOut.print("      " + structVarName + "->static_method."
						+ method2name.get(method)
						+ " = (*env)->GetStaticMethodID(env, " + structVarName
						+ "->class, \"" + method.getName() + "\", \""
						+ getJVMMethodSignature(method) + "\");\n"
						+ "      if ((*env)->ExceptionCheck(env)) break;\n");
			}

			cOut.print("\n");
		}

		// instance methods

		if (instanceMethods.length > 0) {
			cOut.print("      // obtain instance method IDs\n");

			for (int i = 0; i < instanceMethods.length; i++) {
				Method method = instanceMethods[i];
				cOut.print("      " + structVarName + "->method."
						+ method2name.get(method)
						+ " = (*env)->GetMethodID(env, " + structVarName
						+ "->class, \"" + method.getName() + "\", \""
						+ getJVMMethodSignature(method) + "\");\n"
						+ "      if ((*env)->ExceptionCheck(env)) break;\n");
			}

			cOut.print("\n");
		}

		// trailer and error handler

		cOut.print("      // we're done\n" + "      return " + structVarName
				+ ";\n" + "   }\n" + "\n" + "   // error handler\n"
				+ "   (*env)->ExceptionDescribe(env);\n"
				+ "   (*env)->ExceptionClear(env);\n" + "   free_"
				+ structTypeName + "(env, " + structVarName + ");\n"
				+ "   return NULL;\n" + "}\n" + "\n");
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: " + CAPIGenerator.class.getName()
					+ " file.h file.c common_include.h");
			System.exit(-1);
		}

		File hFile = new File(args[0]);
		File cFile = new File(args[1]);
		File hIncluded = new File(args[2]);

		PrintWriter hOut = null;
		PrintWriter cOut = null;
		try {
			hOut = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
					hFile)));
			cOut = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
					cFile)));

			hOut.print("/**\n" + " * " + hFile.getName()
					+ " - autogenerated C <-> Java bindings\n" + " */\n" + "\n"
					+ "#include <jni.h>\n" + "\n");

			cOut.print("/**\n" + " * " + cFile.getName()
					+ " - autogenerated C <-> Java bindings\n" + " */\n" + "\n"
					+ "#include \"" + hIncluded.getPath() + "\"\n" + "\n");

			new CAPIGenerator(FuseGetattr.class).generateClassAPI(hOut, cOut,
					false, false);
			new CAPIGenerator(FuseFSDirEnt.class).generateClassAPI(hOut, cOut,
					false, false);
			new CAPIGenerator(FuseFSDirFiller.class).generateClassAPI(hOut,
					cOut, false, false);
			new CAPIGenerator(FuseStatfs.class).generateClassAPI(hOut, cOut,
					false, false);
			new CAPIGenerator(FuseSize.class).generateClassAPI(hOut, cOut,
					false, false);
			new CAPIGenerator(FuseOpen.class).generateClassAPI(hOut, cOut,
					false, false);
			new CAPIGenerator(FuseContext.class).generateClassAPI(hOut, cOut,
					false, false);
			new CAPIGenerator(FuseFS.class).generateClassAPI(hOut, cOut, false,
					false);
			new CAPIGenerator(ByteBuffer.class).generateClassAPI(hOut, cOut,
					false, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (hOut != null)
				hOut.close();
			if (cOut != null)
				cOut.close();
		}
	}

	/**
	 * Java VM Type Signatures
	 * 
	 * Type Signature Java Type
	 * 
	 * Z boolean B byte C char S short I int J long F float D double L
	 * fully-qualified-class ; fully-qualified-class [ type type[]
	 * 
	 * ( arg-types ) ret-type method type
	 */
	private String getJVMTypeSignature(Class<?> clazz) {
		return appendJVMTypeSignature(clazz, new StringBuffer()).toString();
	}

	private String getJVMTypeSignatures(Class<?>[] classes) {
		return appendJVMTypeSignatures(classes, new StringBuffer()).toString();
	}

	private String getJVMMethodSignature(Method method) {
		return appendJVMMethodSignature(method, new StringBuffer()).toString();
	}

	private String getJVMConstructorSignature(Constructor<?> constructor) {
		return appendJVMConstructorSignature(constructor, new StringBuffer())
				.toString();
	}

	private StringBuffer appendJVMTypeSignature(Class<?> clazz,
			StringBuffer buff) {
		while (clazz.isArray()) {
			buff.append('[');
			clazz = clazz.getComponentType();
		}

		if (clazz == Void.TYPE)
			buff.append("V");
		else if (clazz == Boolean.TYPE)
			buff.append("Z");
		else if (clazz == Byte.TYPE)
			buff.append("B");
		else if (clazz == Character.TYPE)
			buff.append("C");
		else if (clazz == Short.TYPE)
			buff.append("S");
		else if (clazz == Integer.TYPE)
			buff.append("I");
		else if (clazz == Long.TYPE)
			buff.append("J");
		else if (clazz == Float.TYPE)
			buff.append("F");
		else if (clazz == Double.TYPE)
			buff.append("D");
		else
			buff.append("L").append(clazz.getName().replace('.', '/'))
					.append(";");

		return buff;
	}

	private StringBuffer appendJVMTypeSignatures(Class<?>[] classes,
			StringBuffer buff) {
		for (int i = 0; i < classes.length; i++)
			appendJVMTypeSignature(classes[i], buff);

		return buff;
	}

	private StringBuffer appendJVMMethodSignature(Method method,
			StringBuffer buff) {
		buff.append("(");

		appendJVMTypeSignatures(method.getParameterTypes(), buff);

		buff.append(")");

		appendJVMTypeSignature(method.getReturnType(), buff);

		return buff;
	}

	private StringBuffer appendJVMConstructorSignature(
			Constructor<?> constructor, StringBuffer buff) {
		buff.append("(");

		appendJVMTypeSignatures(constructor.getParameterTypes(), buff);

		buff.append(")V");

		return buff;
	}

	private String mangle(String str) {
		char[] chars = str.toCharArray();

		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];
			if ((i == 0 && !Character.isJavaIdentifierStart(c))
					|| (i > 0 && !Character.isJavaIdentifierPart(c)))
				chars[i] = '_';
		}

		return new String(chars);
	}

	private String getMethodName(Class<?> returnType, String methodName,
			Class<?>[] argumentTypes) {
		String prepend = (returnType != null ? getJVMTypeSignature(returnType)
				+ "__" : "");
		if (argumentTypes == null || argumentTypes.length == 0)
			return mangle(prepend + methodName);
		else
			return mangle(prepend + methodName + "__"
					+ getJVMTypeSignatures(argumentTypes));
	}
}
