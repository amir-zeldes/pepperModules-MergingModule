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
package de.hu_berlin.german.korpling.saltnpepper.pepperModules.mergingModules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.emf.common.util.URI;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hu_berlin.german.korpling.saltnpepper.pepper.common.DOCUMENT_STATUS;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.MappingSubject;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperManipulator;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapper;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.exceptions.PepperModuleException;
import de.hu_berlin.german.korpling.saltnpepper.salt.SaltFactory;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpus;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SMetaAnnotatableElement;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SMetaAnnotation;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SNode;
import java.util.LinkedList;
import java.util.Vector;

/**
 * 
 * @author Florian Zipser
 * @version 1.0
 * 
 */
@Component(name = "MergerComponent", factory = "PepperManipulatorComponentFactory")
public class Merger extends BaseManipulator implements PepperManipulator {
	public static final String MODULE_NAME="Merger";
	
	private static final Logger logger = LoggerFactory.getLogger(MODULE_NAME);

	public Merger() {
		super();
		setName(MODULE_NAME);
		setProperties(new MergerProperties());
	}

	/**
	 * A table containing the import order for {@link SElementId} corresponding
	 * to {@link SDocument} and {@link SCorpus} nodes corresponding to the
	 * {@link SCorpusGraph} they are contained in.
	 **/
	private Map<SCorpusGraph, List<SElementId>> importOrder = null;


	/**
	 * Creates a table of type {@link SNodeByIDStorage}, which contains a slot of
	 * matching elements as value. The key is the {@link SElementId}. Only real
	 * existing elements are contained in table.
	 */
	protected synchronized void createMapping() {
		if (mappingTable == null) {
			setBaseCorpusStructure(getSaltProject().getSCorpusGraphs().get(0));

			// initialize importOrder
			importOrder = new HashMap<SCorpusGraph, List<SElementId>>();
			for (SCorpusGraph graph : getSaltProject().getSCorpusGraphs()) {
				importOrder.put(graph, new ArrayList<SElementId>());
			}

			mappingTable = new SNodeByIDStorage();
			// TODO add mapping properties to table
			for (SCorpusGraph graph : getSaltProject().getSCorpusGraphs()) {
				if (graph.getSCorpora().size() != 0) {
					for (SCorpus sCorpus : graph.getSCorpora()) {
						// TODO check if sCorpus.getSId() is contained in
						// mapping properties
						mappingTable.put(sCorpus.getSId(), sCorpus);
					}
					for (SDocument sDocument : graph.getSDocuments()) {

						// TODO check if sDocument.getSId() is contained in
						// mapping properties
						mappingTable.put(sDocument.getSId(), sDocument);
					}
				}
			}
			List<List<List<SNode>>> listOfLists = new ArrayList<List<List<SNode>>>(getSaltProject().getSCorpusGraphs().size());
			for (int i = 0; i < getSaltProject().getSCorpusGraphs().size(); i++) {
				listOfLists.add(new ArrayList<List<SNode>>());
			}

			for (String key : mappingTable.keySet()) {
				List<SNode> nodes = mappingTable.get(key);
				listOfLists.get(nodes.size() - 1).add(nodes);
			}

			for (int i = getSaltProject().getSCorpusGraphs().size(); i > 0; i--) {
				List<List<SNode>> list = listOfLists.get(i - 1);
				for (List<SNode> nodes : list) {
					for (SNode node : nodes) {
						if (node instanceof SDocument) {
							importOrder.get(((SDocument) node).getSCorpusGraph()).add(node.getSElementId());
						}
					}
				}
			}
		}
	}

	@Override
	protected List<SNode> getMappableSlot(SElementId sElementId) {
		return mappingTable.get(sElementId.getSId());
	}
	
	

	@Override
	protected List<SElementId> getOrCreateGivenSlot(SElementId sElementId) {
		List<SElementId> givenSlot = givenSlots.get(sElementId.getSId());
		if (givenSlot == null) {
			givenSlot = new Vector<>();
			givenSlots.put(sElementId.getSId(), givenSlot);
		}
		return givenSlot;
	}

	

	/**
	 * Moves all {@link SMetaAnnotation}s from <em>source</em> object to passed
	 * <em>target</em> object.
	 * 
	 * @param source
	 * @param target
	 */
	protected static void moveSMetaAnnotations(SMetaAnnotatableElement source, SMetaAnnotatableElement target) {
		if ((source != null) && (target != null)) {
			SMetaAnnotation sMeta = null;
			int offset = 0;
			while (source.getSMetaAnnotations().size() > offset) {
				sMeta = source.getSMetaAnnotations().get(0);
				if (target.getSMetaAnnotation(SaltFactory.eINSTANCE.createQName(sMeta.getSNS(), sMeta.getSName())) == null) {
					target.addSMetaAnnotation(sMeta);
				} else {
					offset++;
				}
			}
		}
	}

	/**
	 * Creates an import order for each {@link SCorpusGraph} object. The order
	 * for given {@link SCorpusGraph} objects is very similar or equal, in case
	 * they contain the same {@link SDocument}s (the ones to be merged).
	 */
	@Override
	public List<SElementId> proposeImportOrder(SCorpusGraph sCorpusGraph) {
		List<SElementId> retVal = null;
		if (sCorpusGraph != null) {
			if (getSaltProject().getSCorpusGraphs().size() > 1) {
				createMapping();
				if (importOrder != null) {
					retVal = importOrder.get(sCorpusGraph);
				}
			}
		}
		return (retVal);
	}


	/**
	 * For each {@link SCorpus} and {@link SDocument} in mapping table which has
	 * no corresponding one in base corpus-structure one is created.
	 */
	@Override
	protected final void enhanceBaseCorpusStructure() {
		if(mappingTable != null) {
			Set<String> keys = mappingTable.keySet();
			if ((keys != null) && (keys.size() > 0)) {
				for (String key : keys) {
					List<SNode> slot = mappingTable.get(key);
					boolean noBase = true;
					boolean isDoc = true;
					for (SNode node : slot) {
						if (node != null) {
							if (node instanceof SCorpus) {
								isDoc = false;
								if (((SCorpus) node).getSCorpusGraph().equals(getBaseCorpusStructure())) {
									noBase = false;
									break;
								}
							} else if (node instanceof SDocument) {
								isDoc = true;
								if (((SDocument) node).getSCorpusGraph().equals(getBaseCorpusStructure())) {
									noBase = false;
									break;
								}
							}
						}
					}
					if (noBase) {
						if (isDoc) {
							getBaseCorpusStructure().createSCorpus(URI.createURI(key).trimSegments(1));
							SDocument doc= getBaseCorpusStructure().createSDocument(URI.createURI(key));
							doc.setSDocumentGraph(SaltFactory.eINSTANCE.createSDocumentGraph());
						} else {
							getBaseCorpusStructure().createSCorpus(URI.createURI(key));
						}
					}
				}
			}
		}
	}
	
	@Override
	public BaseMapper newMapperInstance() {
		return new MergerMapper();
	}
	
	
}
