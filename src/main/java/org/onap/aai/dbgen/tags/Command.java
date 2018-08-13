package org.onap.aai.dbgen.tags;

@FunctionalInterface
interface Command {
	public abstract void execute ( ) throws Exception;
}
