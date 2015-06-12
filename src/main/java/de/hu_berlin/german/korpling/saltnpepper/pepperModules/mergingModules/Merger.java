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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.eclipse.emf.common.util.URI;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hu_berlin.german.korpling.saltnpepper.pepper.common.DOCUMENT_STATUS;
import de.hu_berlin.german.korpling.saltnpepper.pepper.exceptions.PepperFWException;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.DocumentController;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.MappingSubject;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperManipulator;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapper;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapperController;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.exceptions.PepperModuleException;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.impl.PepperManipulatorImpl;
import de.hu_berlin.german.korpling.saltnpepper.salt.SaltFactory;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpus;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SMetaAnnotatableElement;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SMetaAnnotation;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SNode;

/**
 * 
 * @author Florian Zipser
 * @version 1.0
 * 
 */
@Component(name = "MergerComponent", factory = "PepperManipulatorComponentFactory")
public class Merger extends PepperManipulatorImpl implements PepperManipulator {
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
	 * a map containing all mapping partners ({@link SCorpus} and
	 * {@link SDocument} nodes) corresponding to their sId.
	 **/
	protected Multimap mappingTable = null;

	/**
	 * similar to guavas multimap, but can contain values twice (this is
	 * because, equal method of two {@link SDocument}s having the same path but
	 * belong to different {@link SCorpusGraph}s are the same for equals(), but
	 * shouldn't be.)
	 **/
	class Multimap {
		private Map<String, List<SNode>> map = null;

		public Multimap() {
			map = new HashMap<String, List<SNode>>();
		}

		public void put(String sId, SNode sNode) {
			List<SNode> slot = map.get(sId);
			if (slot == null) {
				slot = new ArrayList<SNode>();
				map.put(sId, slot);
			}
			slot.add(sNode);
		}

		public List<SNode> get(String sId) {
			return (map.get(sId));
		}

		@Override
		public String toString() {
			StringBuilder retVal = new StringBuilder();
			for (String key : map.keySet()) {
				retVal.append(key);
				retVal.append("=");
				List<SNode> sNodes = map.get(key);
				if (sNodes != null) {
					int i = 0;
					for (SNode sNode : sNodes) {
						if (i != 0) {
							retVal.append(", ");
						}
						retVal.append(SaltFactory.eINSTANCE.getGlobalId(sNode.getSElementId()));
						i++;
					}
				}
				retVal.append("; ");
			}
			return (retVal.toString());
		}

		public Set<String> keySet() {
			return (map.keySet());
		}
	}

	/**
	 * Determines which {@link SCorpusGraph} is the base corpus graph, in which
	 * everything has to be merged in.
	 **/
	private SCorpusGraph baseCorpusStructure = null;

	/**
	 * Returns the {@link SCorpusGraph} is the base corpus graph, in which
	 * everything has to be merged in.
	 * 
	 * @return
	 */
	public SCorpusGraph getBaseCorpusStructure() {
		return baseCorpusStructure;
	}

	/**
	 * Sets the {@link SCorpusGraph} is the base corpus graph, in which
	 * everything has to be merged in.
	 * 
	 * @param baseCorpusStructure
	 */
	public void setBaseCorpusStructure(SCorpusGraph baseCorpusStructure) {
		this.baseCorpusStructure = baseCorpusStructure;
	}

	/**
	 * Creates a table of type {@link Multimap}, which contains a slot of
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

			int maxSizeOfDocumentGroup = 0;

			mappingTable = new Multimap();
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
						maxSizeOfDocumentGroup = Math.max(maxSizeOfDocumentGroup,
							mappingTable.get(sDocument.getSId()).size());
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

			if (maxSizeOfDocumentGroup >= getModuleController().getJob().getMaxNumberOfDocuments()) {
				/* 
				 The configuration for the maximum number of documents in memory is 
				 too small. If not changed it will result in a dead-lock in the code 
				 as soon as a group of documents is merged which is larger or equal than 
				 the maximal allowed number of documents. "Equal" because the number
				 of documents in memory will be always size(group) + 1 (the new document
				 which is created must be acccounted as well).
        
				 While changing the configuration in code is conflicting with the intention
				 of the user to limit the memory used, a dead-lock would be even worse.
				 */
				logger.warn("Maximal number of documents which can be hold in memory "
					+ "is not sufficient for the merging module and must be increased.");
				getModuleController().getJob().setMaxNumberOfDocuments(
					maxSizeOfDocumentGroup+1);
			}
      
		} // end if mapping table is null
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

	/** This table stores all corresponding mergable {@link SElementId}. */
	private Map<String, List<SElementId>> givenSlots = null;

	/**
	 * For each {@link SCorpus} and {@link SDocument} in mapping table which has
	 * no corresponding one in base corpus-structure one is created.
	 */
	private void enhanceBaseCorpusStructure() {
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

	/**
	 * a set of {@link SElementId} corresponding to documents for which the
	 * merging have not been started
	 **/
	private Set<String> documentsToMerge = new HashSet<String>();

	/**
	 * {@inheritDoc PepperModule#start()} Overrides parent method, to enable the
	 * parallel working in more than one {@link DocumentController} objects at a
	 * time.
	 */
	@Override
	public void start() throws PepperModuleException {
		if (getSaltProject() == null) {
			throw new PepperFWException("No salt project was set in module '" + getName() + ", " + getVersion() + "'.");
		}
		if (mappingTable== null){
			//nothing to be done here
			
			logger.warn("[Merger] Cannot merge corpora or documents, since only one corpus structure is given. ");
			
			boolean isStart = true;
			SElementId sElementId = null;
			DocumentController documentController = null;
			while ((isStart) || (sElementId != null)) {
				isStart = false;
				documentController = this.getModuleController().next();
				if (documentController == null) {
					break;
				}
				sElementId = documentController.getsDocumentId();
				getModuleController().complete(documentController);
			}
			this.end();
			
			return;
		}
		enhanceBaseCorpusStructure();
		if (	(logger.isDebugEnabled())&&
				(mappingTable!= null)){
			StringBuilder mergerMapping = new StringBuilder();
			mergerMapping.append("Computed mapping for merging:\n");
			for (String key : mappingTable.keySet()) {
				List<SNode> partners = mappingTable.get(key);
				mergerMapping.append("\t");
				boolean isFirst = true;
				mergerMapping.append("(");
				for (SNode partner : partners) {
					if (!isFirst) {
						mergerMapping.append(", ");
					} else {
						isFirst = false;
					}
					mergerMapping.append(SaltFactory.eINSTANCE.getGlobalId(partner.getSElementId()));
				}
				mergerMapping.append(")");
				mergerMapping.append("\n");
			}
			logger.debug("[Merger] " + mergerMapping.toString());
		}
		// creating new thread group for mapper threads
		setMapperThreadGroup(new ThreadGroup(Thread.currentThread().getThreadGroup(), this.getName() + "_mapperGroup"));
		givenSlots = new Hashtable<String, List<SElementId>>();
		boolean isStart = true;
		SElementId sElementId = null;
		DocumentController documentController = null;
		while ((isStart) || (sElementId != null)) {
			isStart = false;
			documentController = this.getModuleController().next();
			if (documentController == null) {
				break;
			}
			sElementId = documentController.getsDocumentId();
			getDocumentId2DC().put(SaltFactory.eINSTANCE.getGlobalId(sElementId), documentController);

			List<SNode> mappableSlot = mappingTable.get(sElementId.getSId());
			List<SElementId> givenSlot = givenSlots.get(sElementId.getSId());
			if (givenSlot == null) {
				givenSlot = new Vector<SElementId>();
				givenSlots.put(sElementId.getSId(), givenSlot);
			}
			givenSlot.add(sElementId);
			logger.trace("[Merger] New document has arrived {}. ", SaltFactory.eINSTANCE.getGlobalId(sElementId));
			documentsToMerge.add(SaltFactory.eINSTANCE.getGlobalId(sElementId));

			if (givenSlot.size() < mappableSlot.size()) {
				if (logger.isTraceEnabled()) {
					logger.trace("[Merger] " + "Waiting for further documents, {} documents are in queue. ", documentsToMerge.size());
				}
				documentController.sendToSleep_FORCE();
				// this is a bit hacky, but necessary
				if (documentController.isAsleep()) {
					getModuleController().getJob().releaseDocument(documentController);
				}
				logger.trace("[Merger] " + "Sent document '{}' to sleep, until matching partner(s) was processed. ", documentController.getGlobalId());
			} else if (givenSlot.size() == mappableSlot.size()) {
				try {
					for (SElementId sDocumentId : givenSlot) {
						DocumentController docController = getDocumentId2DC().get(SaltFactory.eINSTANCE.getGlobalId(sDocumentId));
						if (docController == null) {
							throw new PepperModuleException(this, "Cannot find a document controller for document '" + SaltFactory.eINSTANCE.getGlobalId(sDocumentId) + "' in list: " + getDocumentId2DC() + ". ");
						}
						logger.trace("[Merger] Try to wake up document {}. {} documents are currently active. ", docController.getGlobalId(), getModuleController().getJob().getNumOfActiveDocuments());
						// ask for loading a document into main memory and wait
						// if necessary
						getModuleController().getJob().getPermissionForProcessDoument(docController);
						docController.awake();
						logger.trace("[Merger] Successfully woke up document {}. ", docController.getGlobalId());
						documentsToMerge.remove(docController.getGlobalId());
					}
					if (logger.isTraceEnabled()) {
						String docs = "";
						int i = 0;
						for (SElementId sDocumentId : givenSlot) {
							if (i == 0) {
								docs = sDocumentId.getSId();
							} else {
								docs = docs + ", " + SaltFactory.eINSTANCE.getGlobalId(sDocumentId);
							}
							i++;
						}
						logger.trace("[Merger] " + "Wake up all documents corresponding to id '{}': {}", sElementId.getSId(), docs);
					}
					start(sElementId);
				} catch (Exception e) {
					throw new PepperModuleException("Any exception occured while merging documents corresponding to '" + sElementId + "'. ", e);
				}
			} else
				throw new PepperModuleException(this, "This should not have beeen happend and is a bug of module. The problem is, 'givenSlot.size()' is higher than 'mappableSlot.size()'.");
		}

		Collection<PepperMapperController> controllers = null;
		HashSet<PepperMapperController> alreadyWaitedFor = new HashSet<PepperMapperController>();
		// wait for all SDocuments to be finished
		controllers = Collections.synchronizedCollection(this.getMapperControllers().values());
		for (PepperMapperController controller : controllers) {
			try {
				controller.join();
				alreadyWaitedFor.add(controller);
			} catch (InterruptedException e) {
				throw new PepperFWException("Cannot wait for mapper thread '" + controller + "' in " + this.getName() + " to end. ", e);
			}
		}

		Collection<SCorpus> corpora = Collections.synchronizedCollection(getBaseCorpusStructure().getSCorpora());
		for (SCorpus sCorpus : corpora) {
			start(sCorpus.getSElementId());
		}
		
		end();
		
		// only wait for controllers which have been added by end()
		for (PepperMapperController controller : this.getMapperControllers().values()) {
			if (!alreadyWaitedFor.contains(controller)) {
				try {
					controller.join();
				} catch (InterruptedException e) {
					throw new PepperFWException("Cannot wait for mapper thread '" + controller + "' in " + this.getName() + " to end. ", e);
				}
				this.done(controller);
			}
		}
	}
	
	/**
	 * Removes all corpus-structures except the base corpus-structure
	 */
	@Override
	public void end() throws PepperModuleException {
		List<SCorpusGraph> removeCorpusStructures= new ArrayList<SCorpusGraph>();
		for (SCorpusGraph graph: getSaltProject().getSCorpusGraphs()){
			if (graph!= getBaseCorpusStructure()){
				removeCorpusStructures.add(graph);
			}
		}
		if (removeCorpusStructures.size()>0){
			for (SCorpusGraph graph: removeCorpusStructures){
				getSaltProject().getSCorpusGraphs().remove(graph);
			}
		}
		if (removeCorpusStructures.size()!= 1){
			logger.warn("Could not remove all corpus-structures from salt project which are not the base corpus-structure. Left structures are: '"+removeCorpusStructures+"'. ");
		}
	}
	
	/**
	 * Creates a {@link PepperMapper} of type {@link MergerMapper}. Therefore
	 * the table {@link #givenSlots} must contain an entry for the given
	 * {@link SElementId}. The create methods passes all documents and corpora
	 * given in the entire slot to the {@link MergerMapper}.
	 **/
	@Override
	public PepperMapper createPepperMapper(SElementId sElementId) {
		MergerMapper mapper = new MergerMapper();
		if (sElementId.getSIdentifiableElement() instanceof SDocument) {
			if ((givenSlots == null) || (givenSlots.size() == 0)) {
				throw new PepperModuleException(this, "This should not have been happend and seems to be a bug of module. The problem is, that 'givenSlots' is null or empty in method 'createPepperMapper()'");
			}
			List<SElementId> givenSlot = givenSlots.get(sElementId.getSId());
			if (givenSlot == null) {
				throw new PepperModuleException(this, "This should not have been happend and seems to be a bug of module. The problem is, that a 'givenSlot' in 'givenSlots' is null or empty in method 'createPepperMapper()'. The sElementId '" + sElementId + "' was not contained in list: " + givenSlots);
			}
			boolean noBase = true;
			for (SElementId id : givenSlot) {
				MappingSubject mappingSubject = new MappingSubject();
				mappingSubject.setSElementId(id);
				mappingSubject.setMappingResult(DOCUMENT_STATUS.IN_PROGRESS);
				mapper.getMappingSubjects().add(mappingSubject);
				if (getBaseCorpusStructure()== (((SDocument) id.getSIdentifiableElement()).getSCorpusGraph())) {
					noBase = false;
				}
			}
			if (noBase) {// no corpus in slot containing in base
							// corpus-structure was found
				MappingSubject mappingSubject = new MappingSubject();
				SNode baseSNode = getBaseCorpusStructure().getSNode(sElementId.getSId());
				if (baseSNode == null) {
					throw new PepperModuleException(this, "Cannot create a mapper for '" + SaltFactory.eINSTANCE.getGlobalId(sElementId) + "', since no base SNode was found. ");
				}
				mappingSubject.setSElementId(baseSNode.getSElementId());
				mappingSubject.setMappingResult(DOCUMENT_STATUS.IN_PROGRESS);
				mapper.getMappingSubjects().add(mappingSubject);
			}
		} else if (sElementId.getSIdentifiableElement() instanceof SCorpus) {
			List<SNode> givenSlot = mappingTable.get(sElementId.getSId());
			if (givenSlot == null) {
				throw new PepperModuleException(this, "This should not have been happend and seems to be a bug of module. The problem is, that a 'givenSlot' in 'givenSlots' is null or empty in method 'createPepperMapper()'. The sElementId '" + sElementId + "' was not contained in list: " + givenSlots);
			}
			boolean noBase = true;
			for (SNode sCorpus : givenSlot) {
				MappingSubject mappingSubject = new MappingSubject();
				mappingSubject.setSElementId(sCorpus.getSElementId());
				mappingSubject.setMappingResult(DOCUMENT_STATUS.IN_PROGRESS);
				mapper.getMappingSubjects().add(mappingSubject);
				if (getBaseCorpusStructure().equals(((SCorpus) sCorpus).getSCorpusGraph())) {
					noBase = false;
				}
			}
			if (noBase) {// no corpus in slot containing in base
							// corpus-structure was found
				MappingSubject mappingSubject = new MappingSubject();
				mappingSubject.setSElementId(getBaseCorpusStructure().getSNode(sElementId.getSId()).getSElementId());
				mappingSubject.setMappingResult(DOCUMENT_STATUS.IN_PROGRESS);
				mapper.getMappingSubjects().add(mappingSubject);
			}
		}
		mapper.setBaseCorpusStructure(getBaseCorpusStructure());
		return (mapper);
	}
}
