/*
 * Copyright 2008-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.repository.dao;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.repository.ExecutionContextSerializer;

/**
 * @author Thomas Risberg
 * @author Michael Minella
 */
public class XStreamExecutionContextStringSerializerTests {

	ExecutionContextSerializer serializer;

	@Before
	public void onSetUp() throws Exception {
		XStreamExecutionContextStringSerializer serializerDeserializer = new XStreamExecutionContextStringSerializer();
		(serializerDeserializer).afterPropertiesSet();

		serializer = serializerDeserializer;
	}

	@Test
	public void testSerializeAMap() throws Exception {
		Map<String, Object> m1 = new HashMap<String, Object>();
		m1.put("object1", Long.valueOf(12345L));
		m1.put("object2", "OBJECT TWO");
		// Use a date after 1971 (otherwise daylight saving screws up)...
		m1.put("object3", new Date(123456790123L));
		m1.put("object4", new Double(1234567.1234D));

		Map<String, Object> m2 = serializationRoundTrip(m1);

		compareContexts(m1, m2);
	}

	@Test
	public void testComplexObject() throws Exception {
		Map<String, Object> m1 = new HashMap<String, Object>();
		ComplexObject o1 = new ComplexObject();
		o1.setName("02345");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("object1", Long.valueOf(12345L));
		m.put("object2", "OBJECT TWO");
		o1.setMap(m);
		o1.setNumber(new BigDecimal("12345.67"));
		ComplexObject o2 = new ComplexObject();
		o2.setName("Inner Object");
		o2.setMap(m);
		o2.setNumber(new BigDecimal("98765.43"));
		o1.setObj(o2);
		m1.put("co", o1);

		Map<String, Object> m2 = serializationRoundTrip(m1);

		compareContexts(m1, m2);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testNullSerialization() throws Exception {
		serializer.serialize(null, null);
	}

	private void compareContexts(Map<String, Object> m1, Map<String, Object> m2) {
		for (String key : m1.keySet()) {
			System.out.println("m1 = " + m1 + " m2 = " + m2);
			assertEquals("Bad key/value for " + key, m1.get(key), m2.get(key));
		}
	}

	private Map<String, Object> serializationRoundTrip(Map<String, Object> m1) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		serializer.serialize(m1, out);

		String s = out.toString();

		ByteArrayInputStream in = new ByteArrayInputStream(s.getBytes());
		Map<String, Object> m2 = serializer.deserialize(in);
		return m2;
	}

	@SuppressWarnings("unused")
	private static class ComplexObject {
		private String name;
		private BigDecimal number;
		private ComplexObject obj;
		private Map<String,Object> map;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public BigDecimal getNumber() {
			return number;
		}

		public void setNumber(BigDecimal number) {
			this.number = number;
		}

		public ComplexObject getObj() {
			return obj;
		}

		public void setObj(ComplexObject obj) {
			this.obj = obj;
		}

		public Map<String,Object> getMap() {
			return map;
		}

		public void setMap(Map<String,Object> map) {
			this.map = map;
		}


		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ComplexObject that = (ComplexObject) o;

			if (map != null ? !map.equals(that.map) : that.map != null) {
				return false;
			}
			if (name != null ? !name.equals(that.name) : that.name != null) {
				return false;
			}
			if (number != null ? !number.equals(that.number) : that.number != null) {
				return false;
			}
			if (obj != null ? !obj.equals(that.obj) : that.obj != null) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			int result;
			result = (name != null ? name.hashCode() : 0);
			result = 31 * result + (number != null ? number.hashCode() : 0);
			result = 31 * result + (obj != null ? obj.hashCode() : 0);
			result = 31 * result + (map != null ? map.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "ComplexObject [name=" + name + ", number=" + number + "]";
		}
	}

}
