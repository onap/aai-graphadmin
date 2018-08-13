/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017-2018 AT&T Intellectual Property. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */
package org.onap.aai.interceptors.pre;

public final class AAIRequestFilterPriority {
	
	private AAIRequestFilterPriority() {}
	
	public static final int REQUEST_TRANS_LOGGING = 1000;
	
	public static final int HEADER_VALIDATION = 2000;

	public static final int SET_LOGGING_CONTEXT = 3000;

	public static final int HTTP_HEADER = 4000;

	public static final int LATEST = 4250;

	public static final int AUTHORIZATION = 4500;

	public static final int RETIRED_SERVICE = 5000;

	public static final int VERSION = 5500;

	public static final int HEADER_MANIPULATION = 6000;

	public static final int REQUEST_MODIFICATION = 7000;

}
