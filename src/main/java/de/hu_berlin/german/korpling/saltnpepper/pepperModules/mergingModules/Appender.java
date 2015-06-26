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

import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperManipulator;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapper;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import java.util.List;
import org.osgi.service.component.annotations.Component;

/**
 *
 * @author Thomas Krause <krauseto@hu-berlin.de>
 */
@Component(name = "AppenderComponent", factory
    = "PepperManipulatorComponentFactory")
public class Appender extends BaseManipulator implements PepperManipulator {

    public Appender() {
        setName("Appender");
    }

	@Override
	public PepperMapper createPepperMapper(SElementId sElementId) {
		
		if(sElementId.getSIdentifiableElement() instanceof SDocument) {
			AppendMapper mapper = new AppendMapper();
			// TODO: configure mapper
			return mapper;
		} else {	
			return super.createPepperMapper(sElementId);
		}
	}

	@Override
	public List<SElementId> proposeImportOrder(SCorpusGraph sCorpusGraph) {
		
		List<SElementId> result = null;
		
		if(sCorpusGraph != null) {
			// TODO: fill mappingTable
		}
		
		return result;
	}
	
	

	@Override
	protected void enhanceBaseCorpusStructure() {
	}
	
	
}
