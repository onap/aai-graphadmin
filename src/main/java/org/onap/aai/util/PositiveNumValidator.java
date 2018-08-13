package org.onap.aai.util;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class PositiveNumValidator implements IParameterValidator {

	@Override
	public void validate(String name, String value) throws ParameterException {
		int num = Integer.parseInt(value);

		if(num < 0) {
			throw new ParameterException("Parameter " + name + " should be >= 0");
		}
	}
}