/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2026 Deutsche Telekom. All rights reserved.
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

package org.onap.aai.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Turns Spring's scheduling infrastructure on for the long-running GraphAdmin service, and
 * only for it.
 *
 * <p>
 * This deliberately does not live on {@link org.onap.aai.GraphAdminApp}. The batch entry
 * points (GenTester, DataGrooming, SchemaMod, MigrationController, ...) each bootstrap a
 * plain {@code AnnotationConfigApplicationContext} with a blanket
 * {@code scan("org.onap.aai")}, so they pick GraphAdminApp up as a configuration class
 * along with every {@code @Scheduled} bean in the package tree. With
 * {@code @EnableScheduling} on GraphAdminApp those crons therefore also start ticking
 * inside one-shot CLI tools, where DataSnapshotTasks and DataGroomingTasks finish by
 * calling {@code AAISystemExitUtil.systemExitCloseAAIGraph(0)} - shutting the graph and
 * killing the tool's JVM mid-operation, with an exit status of 0 that hides it from any
 * caller.
 *
 * <p>
 * A batch context is never a web context, so gating on that is what keeps the scheduler
 * out of all of them at once.
 */
@Configuration
@ConditionalOnWebApplication
@EnableScheduling
public class ScheduledTasksConfiguration {
}
