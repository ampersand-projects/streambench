/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.util.bytecode;

/**
 * A ModuleClassLoader loads classes from a Module.
 *
 * TODO: This loader keeps the Module alive as long as any classes it loads are
 * alive.  We may wish to load classes eagerly, then discard the Module.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/23/2013
 */
public final class ModuleClassLoader extends ClassLoader {
	private final Module module;
	/**
	 * Creates a new ModuleClassLoader that will load classes from the given
	 * module after delegating to the current thread's context class loader.
	 * @param module the module to load classes from
	 */
	public ModuleClassLoader(Module module) {
		this(module, Thread.currentThread().getContextClassLoader());
	}

	/**
	 * Creates a new ModuleClassLoader that will load classes from the given
	 * module after delegating to the given class loader.
	 * @param module the module to load classes from
	 * @param parent the parent class loader
	 */
	public ModuleClassLoader(Module module, ClassLoader parent) {
		super(parent);
		this.module = module;
	}

	/**
	 * Returns the Module this ModuleClassLoader loads from.
	 * @return the Module this loader loads from
	 */
	public Module getModule() {
		return module;
	}

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		Klass klass = module.getKlass(name);
		if (klass == null)
			throw new ClassNotFoundException(name);
		byte[] bytes = KlassUnresolver.unresolve(klass);
		return defineClass(name, bytes, 0, bytes.length);
	}

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Module m = new Module();
		Klass k = new Klass("foo.MyClass", m.getKlass(Object.class), null, m);
		System.out.println(m.klasses());
		ModuleClassLoader cl = new ModuleClassLoader(m);
		Class<?> l = cl.loadClass(k.getName());
		System.out.println(l);
		System.out.println(l.newInstance());
	}
}
