package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.mapping.BatchesToTableMapper;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.DependencyWorkerHolder;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		DependencyWorker.TaskMessage task;
		boolean isValidInclusionDependency;
	}

	@Getter
	@NoArgsConstructor
	public static class DataLoadingCompletedMessage implements Message {
		private static final long serialVersionUID = -4184858160945792991L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++) {
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
			this.batches.put(id, new ArrayList<>());
			this.fileIds.add(id);
			this.readingComplete.put(id, false);
		}
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<DependencyWorkerHolder> dependencyWorkers;

	private final Map<Integer, List<List<String[]>>> batches = new HashMap<>();

	private final Map<Integer, Boolean> readingComplete = new HashMap<>();

	private final List<Integer> fileIds = new ArrayList<>();

	private final Queue<DependencyWorker.TaskMessage> unassignedTasks = new LinkedList<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(DataLoadingCompletedMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		final int fileId = message.getId();

		if (!message.getBatch().isEmpty()) { //reading not done yet, tell input reader to read more
			this.batches.get(fileId).add(message.getBatch());
			this.inputReaders.get(fileId).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
			return this;
		}

		//if we reach this point, the reading is done for a specific file id, since we got an empty batch
		this.readingComplete.put(fileId, true);

		//now check, if we read all files
		if (isInputComplete()) { //TODO: Collecting the data can be extracted into other actor
			this.getContext().getSelf().tell(new DependencyMiner.DataLoadingCompletedMessage());
		}

		return this;
	}

	private boolean isInputComplete() {
		return this.fileIds.stream().allMatch(fileId -> this.readingComplete.get(fileId).equals(true));
	}

	private Behavior<Message> handle(final DataLoadingCompletedMessage message) {
		final List<Table> tables = BatchesToTableMapper.convertToTables(this.headerLines, this.batches);
		this.createUnaryCandidatesAndAddToUnassignedTasks(tables);
		this.assignTaskToNonBusyWorkers();
		return this;
	}

	private void createUnaryCandidatesAndAddToUnassignedTasks(final List<Table> tables) {
		for (final Table tableLeft : tables) {
			for (final Table tableRight : tables) {
				for (final String attributeLeft : tableLeft.getHeader()) {
					final int attributeIndexLeft = tableLeft.getHeader().indexOf(attributeLeft);

					for (final String attributeRight : tableRight.getHeader()) {
						final int attributeIndexRight = tableRight.getHeader().indexOf(attributeRight);

						if (tableLeft.getId() != tableRight.getId() || !attributeLeft.equals(attributeRight)) {
							this.unassignedTasks.add(new DependencyWorker.TaskMessage(
									this.largeMessageProxy,
									tableLeft.getId(),
									tableRight.getId(), attributeLeft, attributeRight,
									tableLeft.getData().get(attributeIndexLeft),
									tableRight.getData().get(attributeIndexRight)));
						}
					}
				}
			}
		}
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.getWorker(dependencyWorker).isPresent()) {
			this.dependencyWorkers.add(DependencyWorkerHolder.builder()
					.dependencyWorker(dependencyWorker)
					.dependencyWorkerLargeMessageProxy(message.getDependencyWorkerLargeMessageProxy())
					.isBusy(false)
					.build());
			this.getContext().watch(dependencyWorker);

			this.assignTaskToNonBusyWorkers();
		}
		return this;
	}

	private void assignTaskToNonBusyWorkers() {
		this.dependencyWorkers.stream().filter(worker -> !worker.isBusy()).forEach(worker -> {
			if (!this.unassignedTasks.isEmpty()) {
				DependencyWorker.TaskMessage task = this.unassignedTasks.poll();
				this.largeMessageProxy.tell(
						new LargeMessageProxy.SendMessage(task, worker.getDependencyWorkerLargeMessageProxy()));
				worker.setBusy(true);
			}
		});
	}

	private Optional<DependencyWorkerHolder> getWorker(ActorRef<DependencyWorker.Message> dependencyWorker) {
		return this.dependencyWorkers.stream()
				.filter(worker -> worker.getDependencyWorker().equals(dependencyWorker))
				.findFirst();
	}

	private Behavior<Message> handle(final CompletionMessage message) {
		if (message.isValidInclusionDependency()) {
			final InclusionDependency ind = getInclusionDependency(message);
			this.resultCollector.tell(new ResultCollector.ResultMessage(List.of(ind)));
		}

		final Optional<DependencyWorkerHolder> workerHolder = this.getWorker(message.getDependencyWorker());
		if (workerHolder.isPresent()) {
			workerHolder.get().setBusy(false);
			this.assignTaskToNonBusyWorkers();
		}

		this.checkIfFinished();

		return this;
	}

	private void checkIfFinished() {
		if (this.unassignedTasks.isEmpty() && this.dependencyWorkers.stream().noneMatch(DependencyWorkerHolder::isBusy)) {
			this.end();
		}
		this.getContext().getLog().info("Unassigned task queue size {}", this.unassignedTasks.size());
	}

	private InclusionDependency getInclusionDependency(CompletionMessage message) {
		final int dependent = message.getTask().getLhsFileId();
		final int referenced = message.getTask().getRhsFileId();
		final File dependentFile = this.inputFiles[dependent];
		final File referencedFile = this.inputFiles[referenced];
		final String[] dependentAttributes = {message.getTask().getLhsColumn()};
		final String[] referencedAttributes = {message.getTask().getRhsColumn()};
		final InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
		return ind;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();

		this.dependencyWorkers.stream()
				.filter(dependencyWorkerHolder -> dependencyWorkerHolder.getDependencyWorker().equals(dependencyWorker))
				.forEach(this.dependencyWorkers::remove);

		return this;
	}
}