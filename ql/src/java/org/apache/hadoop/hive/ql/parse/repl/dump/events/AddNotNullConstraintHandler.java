/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

public class AddNotNullConstraintHandler extends AbstractConstraintEventHandler<AddNotNullConstraintMessage> {
  AddNotNullConstraintHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  AddNotNullConstraintMessage eventMessage(String stringRepresentation) {
    return deserializer.getAddNotNullConstraintMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.debug("Processing#{} ADD_NOTNULLCONSTRAINT_MESSAGE message : {}", fromEventId(),
        eventMessageAsJSON);

    if (shouldReplicate(withinContext)) {
      DumpMetaData dmd = withinContext.createDmd(this);
      dmd.setPayload(eventMessageAsJSON);
      dmd.write();
    }
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_ADD_NOTNULLCONSTRAINT;
  }
}
