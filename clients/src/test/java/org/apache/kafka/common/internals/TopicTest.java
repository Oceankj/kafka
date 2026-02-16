/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.internals;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicTest {

    @Test
    public void shouldAcceptValidTopicNames() {
        String maxLengthString = TestUtils.randomString(249);
        String[] validTopicNames = { "valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_.", "...", maxLengthString };

        for (String topicName : validTopicNames) {
            Topic.validate(topicName);
        }
    }

    @Test
    public void shouldThrowOnInvalidTopicNames() {
        char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        String[] invalidTopicNames = { "", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString) };

        for (String topicName : invalidTopicNames) {
            try {
                Topic.validate(topicName);
                fail("No exception was thrown for topic with invalid name: " + topicName);
            } catch (InvalidTopicException e) {
                // Good
            }
        }
    }

    @Test
    public void shouldRecognizeInvalidCharactersInTopicNames() {
        char[] invalidChars = { '/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=' };

        for (char c : invalidChars) {
            String topicName = "Is " + c + "illegal";
            assertFalse(Topic.containsValidPattern(topicName));
        }
    }

    @Test
    public void testTopicHasCollisionChars() {
        List<String> falseTopics = Arrays.asList("start", "end", "middle", "many");
        List<String> trueTopics = Arrays.asList(
                ".start", "end.", "mid.dle", ".ma.ny.",
                "_start", "end_", "mid_dle", "_ma_ny.");

        for (String topic : falseTopics)
            assertFalse(Topic.hasCollisionChars(topic));

        for (String topic : trueTopics)
            assertTrue(Topic.hasCollisionChars(topic));
    }

    @Test
    public void testUnifyCollisionChars() {
        assertEquals("topic", Topic.unifyCollisionChars("topic"));
        assertEquals("_topic", Topic.unifyCollisionChars(".topic"));
        assertEquals("_topic", Topic.unifyCollisionChars("_topic"));
        assertEquals("__topic", Topic.unifyCollisionChars("_.topic"));
    }

    @Test
    public void testTopicHasCollision() {
        List<String> periodFirstMiddleLastNone = Arrays.asList(".topic", "to.pic", "topic.", "topic");
        List<String> underscoreFirstMiddleLastNone = Arrays.asList("_topic", "to_pic", "topic_", "topic");

        // Self
        for (String topic : periodFirstMiddleLastNone)
            assertTrue(Topic.hasCollision(topic, topic));

        for (String topic : underscoreFirstMiddleLastNone)
            assertTrue(Topic.hasCollision(topic, topic));

        // Same Position
        for (int i = 0; i < periodFirstMiddleLastNone.size(); ++i)
            assertTrue(Topic.hasCollision(periodFirstMiddleLastNone.get(i), underscoreFirstMiddleLastNone.get(i)));

        // Different Position
        Collections.reverse(underscoreFirstMiddleLastNone);
        for (int i = 0; i < periodFirstMiddleLastNone.size(); ++i)
            assertFalse(Topic.hasCollision(periodFirstMiddleLastNone.get(i), underscoreFirstMiddleLastNone.get(i)));
    }

    @Test
    public void shouldReturnTrueForValidName() {
        String maxLengthString = TestUtils.randomString(249);
        String[] validTopicNames = {
                "valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_.", "...", maxLengthString
        };

        for (String topicName : validTopicNames) {
            assertTrue(Topic.isValid(topicName));
        }
    }

    @Test
    public void shouldReturnFalseForInvalidTopicNames() {
        char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        String[] invalidTopicNames = {
                "", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString)
        };

        for (String topicName : invalidTopicNames) {
            assertFalse(Topic.isValid(topicName));
        }
    }

    @Test
    void shouldThrowValidateException() {
        // Arrange
        String invalidTopicName = "invalid@@@"; // invalid
        String logPrefix = "Topic";

        // should throw InvalidTopicException
        InvalidTopicException exception = assertThrows(InvalidTopicException.class, () -> {
            Topic.validate(invalidTopicName, logPrefix, msg -> {
                throw new InvalidTopicException(msg);
            });
        });

        String expectedMessage = logPrefix + " is invalid: " +
                "'" + invalidTopicName + "' contains one or more characters other than " +
                "ASCII alphanumerics, '.', '_' and '-'";
        assertEquals(
                expectedMessage,
                exception.getMessage());
    }

    @Test
    public void shouldIdentifyKnownInternalTopicsAsInternal() {
        assertTrue(Topic.isInternal(Topic.GROUP_METADATA_TOPIC_NAME), "__consumer_offsets should be internal");
        assertTrue(Topic.isInternal(Topic.TRANSACTION_STATE_TOPIC_NAME), "__transaction_state should be internal");
        assertTrue(Topic.isInternal(Topic.SHARE_GROUP_STATE_TOPIC_NAME), "__share_group_state should be internal");
    }

    @Test
    public void shouldNotIdentifyStandardTopicsAsInternal() {
        assertFalse(Topic.isInternal("my-business-topic"), "Standard topic should not be internal");
        assertFalse(Topic.isInternal("kafka-logs"), "Standard topic should not be internal");
    }

    @Test
    public void shouldNotTreatClusterMetadataTopicAsInternal() {
        assertFalse(Topic.isInternal(Topic.CLUSTER_METADATA_TOPIC_NAME),
                "__cluster_metadata should NOT be internal in this version");
    }

    @Test
    public void shouldThrowExceptionWhenCheckingNullTopicForInternal() {
        assertThrows(NullPointerException.class, () -> Topic.isInternal(null));
    }

    @Test
    public void shouldAcceptValidCharactersInTopicNames() {
        String[] validTopics = {
                "valid.topic", // contains .
                "valid_topic", // contains _
                "valid-topic", // contains -
                "ValidTopic123", // contains A-Z, a-z, 0-9
                "12345", // only digits
                "topic" // only lowercase letters
        };

        for (String topic : validTopics) {
            assertTrue(Topic.containsValidPattern(topic), "Topic " + topic + " should be valid");
        }
    }

}
