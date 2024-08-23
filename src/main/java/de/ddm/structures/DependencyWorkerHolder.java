package de.ddm.structures;

import akka.actor.typed.ActorRef;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.DependencyWorker;
import lombok.Builder;
import lombok.Data;

/**
 * @author timo.buechert
 */
@Data
@Builder
public class DependencyWorkerHolder {

    ActorRef<DependencyWorker.Message> dependencyWorker;

    ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;

    boolean isBusy;

}
