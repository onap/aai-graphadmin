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