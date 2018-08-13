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
package org.onap.aai.interceptors;

public final class AAIHeaderProperties {
	
	private AAIHeaderProperties(){}
	
	public static final String REQUEST_CONTEXT = "aai-request-context";
	
	public static final String HTTP_METHOD_OVERRIDE = "X-HTTP-Method-Override";
	
	public static final String TRANSACTION_ID = "X-TransactionId";
	
	public static final String FROM_APP_ID = "X-FromAppId";
	
	public static final String AAI_TX_ID = "X-AAI-TXID";
	
	public static final String AAI_REQUEST = "X-REQUEST";
	
	public static final String AAI_REQUEST_TS = "X-REQUEST-TS";
}
