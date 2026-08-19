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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

public class ScheduledTasksConfigurationTest {

    /**
     * ApplicationContextRunner builds a plain AnnotationConfigApplicationContext, which is
     * exactly what the batch entry points use.
     */
    @Test
    public void schedulingStaysOffInBatchToolContexts() {
        new ApplicationContextRunner()
                .withUserConfiguration(ScheduledTasksConfiguration.class)
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ScheduledTasksConfiguration.class);
                    assertThat(context).doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class);
                });
    }

    @Test
    public void schedulingIsEnabledForTheGraphAdminService() {
        new WebApplicationContextRunner()
                .withUserConfiguration(ScheduledTasksConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(ScheduledTasksConfiguration.class);
                    assertThat(context).hasSingleBean(ScheduledAnnotationBeanPostProcessor.class);
                });
    }
}
