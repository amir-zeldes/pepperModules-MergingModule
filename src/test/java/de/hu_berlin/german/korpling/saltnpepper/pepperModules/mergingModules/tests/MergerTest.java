/**
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package de.hu_berlin.german.korpling.saltnpepper.pepperModules.mergingModules.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.eclipse.emf.common.util.URI;
import org.junit.Before;
import org.junit.Test;

import de.hu_berlin.german.korpling.saltnpepper.pepper.testFramework.PepperManipulatorTest;
import de.hu_berlin.german.korpling.saltnpepper.pepperModules.mergingModules.Merger;
import de.hu_berlin.german.korpling.saltnpepper.salt.SaltFactory;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpus;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;

/**
 * 
 * @author Florian Zipser
 *
 */
public class MergerTest extends PepperManipulatorTest {

	private Merger fixture = null;

	public Merger getFixture() {
		return fixture;
	}

	public void setFixture(Merger fixture) {
		this.fixture = fixture;
		super.setResourcesURI(URI.createFileURI(new File(".").getAbsolutePath()));
	}

	@Before
	public void setUp() {
		setFixture(new Merger());
		getFixture().setSaltProject(SaltFactory.eINSTANCE.createSaltProject());
	}

	/**
	 * Tests the merging on level {@link MERGING_LEVEL#MERGE_DOCUMENTS}:
	 * 
	 * <pre>
	 *   c1    |    c1      |    c1      
	 *   |     |   /  \     |   /  \     
	 *   c2    |  c2   c3   |  c2   c3   
	 *   |     | /  \   |   | /  \   |   
	 *   d1    |d1  d2  d3  |d1  d2  d3
	 * </pre>
	 * 
	 * result (autodetect):
	 * 
	 * <pre>
	 *     c1
	 *    /  \     
	 *   c2   c3   
	 *  /  \   |   
	 * d1  d2  d3
	 * </pre>
	 */
	@Test
	public void test_CorpusStructure_1() {

		SCorpusGraph graph1 = SaltFactory.eINSTANCE.createSCorpusGraph();
		SCorpus c1_test = SaltFactory.eINSTANCE.createSCorpus();
		{
			SCorpus c2 = SaltFactory.eINSTANCE.createSCorpus();
			SDocument d1 = SaltFactory.eINSTANCE.createSDocument();
			d1.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());

			c1_test.setSName("c1");
			c2.setSName("c2");
			d1.setSName("d1");

			graph1.addSNode(c1_test);
			graph1.addSSubCorpus(c1_test, c2);
			graph1.addSDocument(c2, d1);
		}

		SCorpusGraph graph2 = SaltFactory.eINSTANCE.createSCorpusGraph();
		{
			SDocument d1 = SaltFactory.eINSTANCE.createSDocument();
			d1.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
			SDocument d2 = SaltFactory.eINSTANCE.createSDocument();
			d2.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
			SDocument d3 = SaltFactory.eINSTANCE.createSDocument();
			d3.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
			SCorpus c1 = SaltFactory.eINSTANCE.createSCorpus();
			SCorpus c2 = SaltFactory.eINSTANCE.createSCorpus();
			SCorpus c3 = SaltFactory.eINSTANCE.createSCorpus();

			c1.setSName("c1");
			c2.setSName("c2");
			c3.setSName("c3");
			d1.setSName("d1");
			d2.setSName("d2");
			d3.setSName("d3");

			graph2.addSNode(c1);
			graph2.addSSubCorpus(c1, c2);
			graph2.addSSubCorpus(c1, c3);
			graph2.addSDocument(c2, d1);
			graph2.addSDocument(c2, d2);
			graph2.addSDocument(c3, d3);
		}

		SCorpusGraph graph3 = SaltFactory.eINSTANCE.createSCorpusGraph();
		{
			SDocument d1 = SaltFactory.eINSTANCE.createSDocument();
			d1.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
			SDocument d2 = SaltFactory.eINSTANCE.createSDocument();
			d2.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
			SDocument d3 = SaltFactory.eINSTANCE.createSDocument();
			d3.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
			SCorpus c1 = SaltFactory.eINSTANCE.createSCorpus();
			SCorpus c2 = SaltFactory.eINSTANCE.createSCorpus();
			SCorpus c3 = SaltFactory.eINSTANCE.createSCorpus();

			c1.setSName("c1");
			c2.setSName("c2");
			c3.setSName("c3");
			d1.setSName("d1");
			d2.setSName("d2");
			d3.setSName("d3");

			graph3.addSNode(c1);
			graph3.addSSubCorpus(c1, c3);
			graph3.addSSubCorpus(c1, c2);
			graph3.addSDocument(c2, d1);
			graph3.addSDocument(c2, d2);
			graph3.addSDocument(c3, d3);
		}

		this.getFixture().getSaltProject().getSCorpusGraphs().add(graph1);
		this.getFixture().getSaltProject().getSCorpusGraphs().add(graph2);
		this.getFixture().getSaltProject().getSCorpusGraphs().add(graph3);

		this.start();
		/**
		 * <pre>
		 *     c1 
		 *    / \ 
		 *  c2   c3 
		 *  / \   | 
		 * d1 d2 d3
		 * </pre>
		 */
		assertEquals(3, graph1.getSCorpora().size());
		assertEquals(3, graph1.getSDocuments().size());
		assertEquals(2, graph1.getSCorpusRelations().size());
		assertEquals(3, graph1.getSCorpusDocumentRelations().size());

		assertEquals(1, graph1.getSRoots().size());
		assertEquals(2, graph1.getOutEdges(c1_test.getSId()).size());
	}

	/**
	 * Tests the merging on level {@link MERGING_LEVEL#MERGE_DOCUMENTS}:
	 * 
	 * <pre>
	 *   |    c1      |    c1       |  c1          
	 *   |   /  \     |   /  \      |   |          
	 *   |  c2   c3   |  c2   c3    |  c2 
	 *   | /  \   |   | /  \   |    |   |
	 *   |d1  d2  d3  |d1  d2  d3   |  d1
	 * </pre>
	 * 
	 * result (autodetect):
	 * 
	 * <pre>
	 *     c1
	 *    /  \     
	 *   c2   c3   
	 *  /  \   |   
	 * d1  d2  d3
	 * </pre>
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void test_CorpusStructure_2() throws InterruptedException {
		// graph 1
		SCorpusGraph graph1 = SaltFactory.eINSTANCE.createSCorpusGraph();
		SCorpus c1_1 = SaltFactory.eINSTANCE.createSCorpus();
		SCorpus c2_1 = SaltFactory.eINSTANCE.createSCorpus();
		SCorpus c3_1 = SaltFactory.eINSTANCE.createSCorpus();
		SDocument d1_1 = SaltFactory.eINSTANCE.createSDocument();
		SDocument d2_1 = SaltFactory.eINSTANCE.createSDocument();
		SDocument d3_1 = SaltFactory.eINSTANCE.createSDocument();
		c1_1.setSName("c1");
		c2_1.setSName("c2");
		c3_1.setSName("c3");
		d1_1.setSName("d1");
		d2_1.setSName("d2");
		d3_1.setSName("d3");
		c1_1.createSMetaAnnotation(null, "anno1", "someValue");
		c2_1.createSMetaAnnotation(null, "anno1", "someValue");
		c3_1.createSMetaAnnotation(null, "anno1", "someValue");
		d1_1.createSMetaAnnotation(null, "anno1", "someValue");
		d2_1.createSMetaAnnotation(null, "anno1", "someValue");
		d3_1.createSMetaAnnotation(null, "anno1", "someValue");
		graph1.addSNode(c1_1);
		graph1.addSSubCorpus(c1_1, c2_1);
		graph1.addSSubCorpus(c1_1, c3_1);
		graph1.addSDocument(c2_1, d1_1);
		graph1.addSDocument(c2_1, d2_1);
		graph1.addSDocument(c3_1, d3_1);
		this.getFixture().getSaltProject().getSCorpusGraphs().add(graph1);

		// graph2
		SCorpusGraph graph2 = SaltFactory.eINSTANCE.createSCorpusGraph();
		SCorpus c1_2 = SaltFactory.eINSTANCE.createSCorpus();
		SCorpus c2_2 = SaltFactory.eINSTANCE.createSCorpus();
		SCorpus c3_2 = SaltFactory.eINSTANCE.createSCorpus();
		SDocument d1_2 = SaltFactory.eINSTANCE.createSDocument();
		SDocument d2_2 = SaltFactory.eINSTANCE.createSDocument();
		SDocument d3_2 = SaltFactory.eINSTANCE.createSDocument();
		c1_2.setSName("c1");
		c2_2.setSName("c2");
		d1_2.setSName("d1");
		d2_2.setSName("d2");
		c3_2.setSName("c3");
		d3_2.setSName("d3");
		c1_2.createSMetaAnnotation(null, "anno2", "someValue");
		c2_2.createSMetaAnnotation(null, "anno2", "someValue");
		d2_2.createSMetaAnnotation(null, "anno2", "someValue");
		d1_2.createSMetaAnnotation(null, "anno2", "someValue");
		c3_2.createSMetaAnnotation(null, "anno2", "someValue");
		d3_2.createSMetaAnnotation(null, "anno2", "someValue");
		graph2.addSNode(c1_2);
		graph2.addSSubCorpus(c1_2, c2_2);
		graph2.addSSubCorpus(c1_2, c3_2);
		graph2.addSDocument(c2_2, d1_2);
		graph2.addSDocument(c2_2, d2_2);
		graph2.addSDocument(c3_2, d3_2);
		this.getFixture().getSaltProject().getSCorpusGraphs().add(graph2);

		// graph3
		SCorpusGraph graph3 = SaltFactory.eINSTANCE.createSCorpusGraph();
		SCorpus c1_3 = SaltFactory.eINSTANCE.createSCorpus();
		SCorpus c2_3 = SaltFactory.eINSTANCE.createSCorpus();
		SDocument d1_3 = SaltFactory.eINSTANCE.createSDocument();
		c1_3.setSName("c1");
		c2_3.setSName("c2");
		d1_3.setSName("d1");
		c1_3.createSMetaAnnotation(null, "anno3", "someValue");
		c2_3.createSMetaAnnotation(null, "anno3", "someValue");
		d1_3.createSMetaAnnotation(null, "anno3", "someValue");
		graph3.addSNode(c1_3);
		graph3.addSSubCorpus(c1_3, c2_3);
		graph3.addSDocument(c2_3, d1_3);
		this.getFixture().getSaltProject().getSCorpusGraphs().add(graph3);

		this.start();

		/**
		 * <pre>
		 *     c1 
		 *    / \ 
		 *   c2 c3 
		 *  / \ | 
		 * d1 d2 d3
		 * </pre>
		 */
		assertEquals(3, graph1.getSCorpora().size());
		assertEquals(3, graph1.getSDocuments().size());
		assertEquals(2, graph1.getSCorpusRelations().size());
		assertEquals(3, graph1.getSCorpusDocumentRelations().size());

		assertEquals("all meta-annotations: " + c1_1.getSMetaAnnotations(), 3, c1_1.getSMetaAnnotations().size());
		assertEquals("all meta-annotations: " + c2_1.getSMetaAnnotations(), 3, c2_1.getSMetaAnnotations().size());
		assertEquals("all meta-annotations: " + c3_1.getSMetaAnnotations(), 2, c3_1.getSMetaAnnotations().size());
		assertEquals("all meta-annotations: " + d1_1.getSMetaAnnotations(), 3, d1_1.getSMetaAnnotations().size());
		assertEquals("all meta-annotations: " + d2_1.getSMetaAnnotations(), 2, d2_1.getSMetaAnnotations().size());
		assertEquals("all meta-annotations: " + d3_1.getSMetaAnnotations(), 2, d3_1.getSMetaAnnotations().size());
	}
}
