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

package org.onap.aai.audit;

import com.google.gson.JsonObject;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.onap.aai.AAISetup;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.exceptions.EdgeRuleNotFoundException;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.rest.client.ApertureService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class AuditGraphson2SqlTest extends AAISetup {

	
	private AuditGraphson2Sql auditG2S;
	
	@Autowired 
	private EdgeIngestor ei;

	@Mock
	private ApertureService apertureServiceMock;

	@Before
	public void setUp() {
		
		auditG2S = new AuditGraphson2Sql(ei, schemaVersions, loaderFactory, apertureServiceMock);
		    
	}

    @Test
    public void testCompareGood() throws IOException, EdgeRuleNotFoundException {
    	String outDir = "logs/data/audit";
    	Boolean resultOk = true;
    	HashMap <String,Integer> gHash = new HashMap <String,Integer> ();
    	gHash.put("tbl2", 5);
    	gHash.put("tbl-x", 6);
    	gHash.put("edge__tbly__tblX", 7);
    	gHash.put("edge__tblZ__tblX", 8);
    	gHash.put("tbl-only-in-g", 88);
    	
    	HashMap <String,Integer> sEdgeHash = new HashMap <String,Integer> ();
    	sEdgeHash.put("edge__tbly__tblX", 7);
    	sEdgeHash.put("edge__tblZ__tblX", 8);
    	
    	HashMap <String,Integer> sNodeHash = new HashMap <String,Integer> ();
    	sNodeHash.put("tbl2", 5);
    	sNodeHash.put("tbl-x", 6);
    	sNodeHash.put("tbl-only-in-sql", 89);
    	
    	String titleInfo = "Comparing data from GraphSon file: " +
        		"fileXYZ.202001223344, and Aperture data using timeStamp = [987654321]"; 
        	
    	try {
    		auditG2S.compareResults(gHash,sNodeHash,sEdgeHash,outDir,titleInfo);
    	}
    	catch ( Exception e ) {
    		System.out.println("ERROR - got this exception: " + e.getMessage());
    		resultOk = false;
    	}   	
    	assertTrue(resultOk);  	
    }
	
    @Test
    public void testCompareResMissingRecords() throws IOException, EdgeRuleNotFoundException {
    	
    	String outDir = "logs/data/audit";
    	Boolean resultOk = true;
    	HashMap <String,Integer> gHash = new HashMap <String,Integer> ();
    	gHash.put("tbl2", 5);
    	gHash.put("tbl-x", 6);
    	gHash.put("edge__tblZ__tblX", 7);
    	gHash.put("edge__tblZ__tblX", 8);
    	gHash.put("tbl-only-in-g", 88);
    	HashMap <String,Integer> sNodeHash = new HashMap <String,Integer> ();
    	HashMap <String,Integer> sEdgeHash = new HashMap <String,Integer> ();
    	
    	String titleInfo = "Comparing data from GraphSon file: " +
        		"fileXYZ.202001223344, and Aperture data using timeStamp = [987654321]"; 
        		  	
    	try {
    		auditG2S.compareResults(gHash,sNodeHash,sEdgeHash,outDir,titleInfo);
    	}
    	catch ( Exception e ) {
    		System.out.println("ERROR - got this exception: " + e.getMessage());
    		resultOk = false;
    	}
    	
    	assertTrue(resultOk);   	
    }
    
    @Test
    public void testGetDateTimeStamp() {
    	long dts = 0;
    	String fName = "xxxxx.yyyy.202003220000";
    	try {
    		dts = auditG2S.getTimestampFromFileName(fName); 
    	} catch (Exception e) {
    		System.out.println(" threw Exception e = " + e.getMessage());
    	}

    	assertEquals(1577595600000L, dts);
    }
    
    @Test
    public void testTimePieceGood() {
    	String dtPiece = "";
    	String fName = "xxxxx.yyyy.222233445566";
    	try {
    		dtPiece = auditG2S.getDateTimePieceOfFileName(fName); 
    	} catch (Exception e) {
    		System.out.println(" threw Exception e = " + e.getMessage());
    	}
    	assertEquals( "222233445566", dtPiece);
    }
    
    @Test
    public void testTimePieceGoodStill() {
    	String dtPiece = "";
    	String fName = "x.222233445566";
    	try {
    		dtPiece = auditG2S.getDateTimePieceOfFileName(fName); 
    	} catch (Exception e) {
    		System.out.println(" threw Exception e = " + e.getMessage());
    	}
    	assertEquals(dtPiece, "222233445566");
    }
    
    @Test
    public void testTimePieceNotime() {
    	String fName = "xxxxx.yyyy";
    	Boolean resultOk = true;
    	try {
    		auditG2S.getDateTimePieceOfFileName(fName); 
    	} catch (Exception e) {
    		System.out.println(" threw Exception e = " + e.getMessage());
    		resultOk = false;
    	}
    	assertFalse(resultOk);
    }
    
    @Test
    public void testTimePieceTooShort() {
    	String fName = "xxxxx.yyyy.22223";
    	Boolean resultOk = true;
    	try {
    		auditG2S.getDateTimePieceOfFileName(fName); 
    	} catch (Exception e) {
    		System.out.println(" threw Exception e = " + e.getMessage());
    		resultOk = false;
    	}
    	assertFalse(resultOk);
    }
    

    @Test
    public void testGetCounts() throws IOException, EdgeRuleNotFoundException {
    	Boolean resultOk = true;
    	String dbName = "narad";
    	String fileNamePart = "dataSnapshot.graphSON.201908151845";
    	String srcDir = "src/test/resources/audit/";
    	HashMap <String,Integer> resHash = new HashMap <String,Integer> ();
    	try {
    		resHash = auditG2S.getCountsFromGraphsonSnapshot(dbName, fileNamePart, srcDir);
    	}
    	catch ( Exception e ) {
    		System.out.println("ERROR - got this exception: " + e.getMessage());
    		resultOk = false;
    	}
    	
    	assertTrue(resultOk);   	
    }
    
   
    @Test
    public void testGoodRun() throws IOException, EdgeRuleNotFoundException {
    	
    	String [] argVals = {};

    	// this is the tStamp that would go
		//     with this file name: "dataSnapshot.graphSON.201908151845"
		Long tStamp = 1565842725000L;


		JsonObject jVal = new JsonObject();
		jVal.addProperty("autonomous-system",5);
		jVal.addProperty("pnf",7);


		String dbn = "narad_relational";
		String resStr = "";
    	try {
			when(apertureServiceMock.runAudit(anyLong(), anyString())).thenReturn(jVal);
    		resStr = auditG2S.runAudit("narad",
					"dataSnapshot.graphSON.201908151845",
					"src/test/resources/audit/");
    	}
    	catch ( Exception e ) {
    		System.out.println("ERROR - got this exception: " + e.getMessage());
			resStr = "Error";
    	}

    	assertTrue( resStr.startsWith("Audit ran and logged") );

    }
    
    @Test
    public void testRunWithBadParam() throws IOException, EdgeRuleNotFoundException {
    	
    	String resStr = "";
    	try {
    		resStr = auditG2S.runAudit("narad",
					"bogusFileName",
					"src/test/resources/audit/");
    	}
    	catch ( Exception e ) {
    		System.out.println("ERROR - got this exception: " + e.getMessage());
    		resStr = "Error";
    	}

		assertTrue( resStr.equals("Error") );
    }


	@Test
	public void testGetCountsFromJson() throws IOException, EdgeRuleNotFoundException {

		JsonObject sqlJsonData = new JsonObject ();
		sqlJsonData.addProperty("tableName1", 4);
		sqlJsonData.addProperty("tableName2", 5);
		sqlJsonData.addProperty("tableName3", 6);
		sqlJsonData.addProperty("tableName4", 7);

		HashMap <String,Integer> results1 = new HashMap <String,Integer> ();
		HashMap <String,Integer> results2 = new HashMap <String,Integer> ();
		try {
			results1 = auditG2S.getNodeCountsFromSQLRes(sqlJsonData);
			results2 = auditG2S.getNodeCountsFromSQLRes(sqlJsonData);
		}
		catch ( Exception e ) {
			System.out.println("ERROR - got this exception: " + e.getMessage());
		}

		assertEquals(4, results1.size());
		assertTrue( results1.containsKey("tableName3") );
		assertTrue( results1.get("tableName4") == 7);
		assertEquals(4, results2.size());
	}
    
}
