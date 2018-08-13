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
/**
 * <b>Interceptors</b> package is subdivided to pre and post interceptors
 * If you want to add an additional interceptor you would need to add
 * the priority level to AAIRequestFilterPriority or AAIResponsePriority
 * to give a value which indicates the order in which the interceptor
 * will be triggered and also you will add that value like here
 *
 * <pre>
 *     <code>
 *         @Priority(AAIRequestFilterPriority.YOUR_PRIORITY)
 *         public class YourInterceptor extends AAIContainerFilter implements ContainerRequestFilter {
 *
 *         }
 *     </code>
 * </pre>
 */
package org.onap.aai.interceptors;