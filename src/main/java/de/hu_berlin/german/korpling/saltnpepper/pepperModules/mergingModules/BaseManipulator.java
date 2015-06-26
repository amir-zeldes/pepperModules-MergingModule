/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
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
package de.hu_berlin.german.korpling.saltnpepper.pepperModules.mergingModules;

import de.hu_berlin.german.korpling.saltnpepper.pepper.common.DOCUMENT_STATUS;
import de.hu_berlin.german.korpling.saltnpepper.pepper.exceptions.PepperFWException;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.DocumentController;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.MappingSubject;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapper;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapperController;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.exceptions.PepperModuleException;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.impl.PepperManipulatorImpl;
import de.hu_berlin.german.korpling.saltnpepper.salt.SaltFactory;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpus;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SNode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Thomas Krause <krauseto@hu-berlin.de>
 */
public abstract class BaseManipulator extends PepperManipulatorImpl {
	
	private final static Logger logger = LoggerFactory.getLogger(BaseManipulator.class);
	
	/**
	 * a map containing all mapping partners ({@link SCorpus} and
	 * {@link SDocument} nodes) corresponding to their sId.
	 **/
	protected SNodeByIDStorage mappingTable = null;
	/** This table stores all corresponding mergable {@link SElementId}. */
	protected Map<String, List<SElementId>> givenSlots = null;
	/**
	 * a set of {@link SElementId} corresponding to documents for which the
	 * merging have not been started
	 **/
	protected Set<String> documentsToMerge = new HashSet<>();
	/**
	 * Determines which {@link SCorpusGraph} is the base corpus graph, in which
	 * everything has to be merged in.
	 **/
	protected SCorpusGraph baseCorpusStructure = null;

	
	protected abstract void enhanceBaseCorpusStructure();
	
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
		if (mappingTable == null) {
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
		if ((logger.isDebugEnabled()) && (mappingTable != null)) {
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
		givenSlots = new Hashtable<>();
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
			List<SNode> mappableSlot = getMappableSlot(sElementId);
			List<SElementId> givenSlot = getOrCreateGivenSlot(sElementId);
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
			} else {
				throw new PepperModuleException(this, "This should not have beeen happend and is a bug of module. The problem is, 'givenSlot.size()' is higher than 'mappableSlot.size()'.");
			}
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
	
	protected abstract List<SNode> getMappableSlot(SElementId sElementId);
	protected abstract List<SElementId> getOrCreateGivenSlot(SElementId sElementId);

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
	 * Creates a {@link PepperMapper} of type {@link MergerMapper}. Therefore
	 * the table {@link #givenSlots} must contain an entry for the given
	 * {@link SElementId}. The create methods passes all documents and corpora
	 * given in the entire slot to the {@link MergerMapper}.
	 * @param sElementId
	 * @return 
	 **/
	@Override
	public PepperMapper createPepperMapper(SElementId sElementId) {
		BaseMapper mapper = newMapperInstance();
		if (sElementId.getSIdentifiableElement() instanceof SDocument) {
			if ((givenSlots == null) || (givenSlots.size() == 0)) {
				throw new PepperModuleException(this, "This should not have been happend and seems to be a bug of module. The problem is, that 'givenSlots' is null or empty in method 'createPepperMapper()'");
			}
			List<SElementId> givenSlot = getOrCreateGivenSlot(sElementId);
			if (givenSlot == null) {
				throw new PepperModuleException(this, "This should not have been happend and seems to be a bug of module. The problem is, that a 'givenSlot' in 'givenSlots' is null or empty in method 'createPepperMapper()'. The sElementId '" + sElementId + "' was not contained in list: " + givenSlots);
			}
			boolean noBase = true;
			for (SElementId id : givenSlot) {
				MappingSubject mappingSubject = new MappingSubject();
				mappingSubject.setSElementId(id);
				mappingSubject.setMappingResult(DOCUMENT_STATUS.IN_PROGRESS);
				mapper.getMappingSubjects().add(mappingSubject);
				if (getBaseCorpusStructure() == (((SDocument) id.getSIdentifiableElement()).getSCorpusGraph())) {
					noBase = false;
				}
			}
			if (noBase) {
				// no corpus in slot containing in base
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
			if (noBase) {
				// no corpus in slot containing in base
				// corpus-structure was found
				MappingSubject mappingSubject = new MappingSubject();
				mappingSubject.setSElementId(getBaseCorpusStructure().getSNode(sElementId.getSId()).getSElementId());
				mappingSubject.setMappingResult(DOCUMENT_STATUS.IN_PROGRESS);
				mapper.getMappingSubjects().add(mappingSubject);
			}
		}
		mapper.setBaseCorpusStructure(getBaseCorpusStructure());
		return mapper;
	}
	
	public abstract BaseMapper newMapperInstance();

	/**
	 * Removes all corpus-structures except the base corpus-structure
	 */
	@Override
	public void end() throws PepperModuleException {
		List<SCorpusGraph> removeCorpusStructures = new ArrayList<SCorpusGraph>();
		for (SCorpusGraph graph : getSaltProject().getSCorpusGraphs()) {
			if (graph != getBaseCorpusStructure()) {
				removeCorpusStructures.add(graph);
			}
		}
		if (removeCorpusStructures.size() > 0) {
			for (SCorpusGraph graph : removeCorpusStructures) {
				getSaltProject().getSCorpusGraphs().remove(graph);
			}
		}
		if (removeCorpusStructures.size() != 1) {
			logger.warn("Could not remove all corpus-structures from salt project which are not the base corpus-structure. Left structures are: '" + removeCorpusStructures + "'. ");
		}
	}
	
	/**
	 * similar to guavas multimap, but can contain values twice (this is
	 * because, equal method of two {@link SDocument}s having the same path but
	 * belong to different {@link SCorpusGraph}s are the same for equals(), but
	 * shouldn't be.)
	 **/
	public static class SNodeByIDStorage {
		private Map<String, List<SNode>> map = null;

		public SNodeByIDStorage() {
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
  
}
