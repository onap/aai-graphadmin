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
package org.onap.aai.interceptors.post;

/**
 * Response Filter order is done reverse sorted
 * so in the following case the first response filter would be
 * HEADER_MANIPULATION, RESPONSE_TRANS_LOGGING, RESET_LOGGING_CONTEXT,
 * and INVALID_RESPONSE_STATUS
 */
public final class AAIResponseFilterPriority {
	
	private AAIResponseFilterPriority() {}

	public static final int INVALID_RESPONSE_STATUS = 1000;

	public static final int RESET_LOGGING_CONTEXT = 2000;

	public static final int RESPONSE_TRANS_LOGGING = 3000;

	public static final int HEADER_MANIPULATION = 4000;

}
